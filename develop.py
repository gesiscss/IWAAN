from external.wikipedia import WikipediaDV, WikipediaAPI
wikipedia_dv = WikipediaDV(WikipediaAPI(domain='en.wikipedia.org'))
the_page = wikipedia_dv.get_page('The Camp of the Saints')

from wikiwho_wrapper import WikiWho



wikiwho = WikiWho(lng='en')
agg_actions = wikiwho.dv.edit_persistence(the_page.page_id)


editors = wikipedia_dv.get_editors(agg_actions['editor_id'].unique()).rename(columns = {
    'userid': 'editor_id'})

# Merge the namesof the editors to the aggregate actions dataframe
agg_actions = agg_actions.merge(editors[['editor_id', 'name']], on='editor_id')
agg_actions.insert(3, 'editor', agg_actions['name'])
agg_actions = agg_actions.drop(columns=['name'])
agg_actions['editor'] = agg_actions['editor'].fillna("Unregistered")

all_content = wikiwho.dv.all_content(the_page['page_id'])



revisions = wikiwho.dv.rev_ids_of_article(the_page['page_id'])

from metrics.conflict import ConflictManager
calculator = ConflictManager(all_content, revisions)
calculator.calculate()

editors_conflicts = calculator.get_conflict_score_per_editor()

editors['editor_id'] = editors['editor_id'].astype(str)
if len(editors_conflicts) > 0:
    editors_conflicts = editors[['editor_id', 'name']].merge(editors_conflicts, 
                                                right_index=True, left_on='editor_id').set_index('editor_id')



from visualization.owned_listener import OwnedListener
owned = calculator.all_actions
listener = OwnedListener(owned, '28921814')


listener.listen(
	_range = (owned['rev_time'].dt.date.min(), owned['rev_time'].dt.date.max()),
    granularity='Monthly',
     trace='Tokens Owned (%)')

import ipdb; ipdb.set_trace()  # breakpoint b86e2bcc //
