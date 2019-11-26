# functions for hiding
from IPython.display import HTML
import random

def hide_toggle(for_next=False, hiding_text='Toggle show/hide'):
    this_cell = """$('div.cell.code_cell.rendered.selected')"""
    next_cell = this_cell + '.next()'

    toggle_text = hiding_text  # text shown on toggle link
    target_cell = this_cell  # target cell to control with toggle
    js_hide_current = ''  # bit of JS to permanently hide code in current cell (only when toggling next cell)

    if for_next:
        target_cell = next_cell
        toggle_text += ' next cell'
        js_hide_current = this_cell + '.find("div.input").hide();'

    js_f_name = 'code_toggle_{}'.format(str(random.randint(1,2**64)))

    html = """
        <script>
            function {f_name}() {{
                {cell_selector}.find('div.input').toggle();
            }}

            {js_hide_current}
        </script>

        <a href="javascript:{f_name}()">{toggle_text}</a>
    """.format(
        f_name=js_f_name,
        cell_selector=target_cell,
        js_hide_current=js_hide_current, 
        toggle_text=toggle_text
    )

    return HTML(html)

def hide_toggle2(for_next=False, for_next_next=False, for_next_next_next=False, hiding_text='Toggle show/hide'):
    this_cell = """$('div.cell.code_cell.rendered.selected')"""
    next_cell = this_cell + '.next()'
    next_next = this_cell + '.next().next()'
    next_next_next = this_cell + '.next().next().next()'

    toggle_text = hiding_text  # text shown on toggle link
    target_cell = this_cell  # target cell to control with toggle
    js_hide_current = ''  # bit of JS to permanently hide code in current cell (only when toggling next cell)

    if for_next:
        target_cell = next_cell
        toggle_text += ' next cell'
        js_hide_current = this_cell + '.find("div.input").hide();'
    
    if for_next_next:
        target_cell = next_next
        toggle_text += ' next next cell'
        js_hide_current = this_cell + '.find("div.input").hide();'

    if for_next_next_next:
        target_cell = next_next_next
        toggle_text += ' next next next cell'
        js_hide_current = this_cell + '.find("div.input").hide();'

    js_f_name = 'code_toggle_{}'.format(str(random.randint(1,2**64)))
    js_f_name2 = 'code_toggle_{}'.format(str(random.randint(1,3**70)))

    html = """
        <script>
            function {f_name}() {{
                {cell_selector}.find('div.input').toggle();
                {cell_selector}.find('div.output').toggle()
            }}           

            {js_hide_current};
            {f_name}()
        </script>
    """.format(
        f_name=js_f_name,
        cell_selector=target_cell,
        js_hide_current=js_hide_current, 
        toggle_text=toggle_text
    )

    return HTML(html)

def hide_cell(hide_code=True):
    if hide_code:
        html = """
                <script>
                    var code_show=true;
                    function code_toggle() {
                    $('div.prompt').hide(); // always hide prompt

                    if (code_show){
                        $('div.input').hide();
                    } else {
                        $('div.input').show();
                    }
                    code_show = !code_show
                    }
                    $( document ).ready(code_toggle);
                </script>
            """
    else:
        html = """
                <script>
                    var code_show=false;
                    function code_toggle() {
                    $('div.prompt').hide(); // always hide prompt

                    if (code_show){
                        $('div.input').hide();
                    } else {
                        $('div.input').show();
                    }
                    code_show = !code_show
                    }
                    $( document ).ready(code_toggle);
               </script>
           """
           
    return display(HTML(html))