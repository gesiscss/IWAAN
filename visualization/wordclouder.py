import numpy as np
from wordcloud import WordCloud as WC

from multidict import MultiDict


class WordCloud(WC):

    def recolor(self, random_state=None, color_func=None, colormap=None):
        if isinstance(random_state, (int, np.integer)):
            random_state = Random(random_state)
        self._check_generated()

        if color_func is None:
            if colormap is None:
                color_func = self.color_func
            else:
                color_func = colormap_color_func(colormap)

        # Here I remove the character so it doesn't get displayed
        # when the wordcloud image is produced
        self.layout_ = [((word_freq[0][:-1], word_freq[1]), font_size, position, orientation,
                         # but I send the full word to the color_func
                         color_func(word=word_freq[0], font_size=font_size,
                                    position=position, orientation=orientation,
                                    random_state=random_state,
                                    font_path=self.font_path))
                        for word_freq, font_size, position, orientation, _
                        in self.layout_]

        return self


class WordClouder(object):

    def __init__(self, words, colors, max_words):
        self.words = words
        self.colors = colors
        self.max_words = max_words

    def get_color_func(self, word, **args):
        return self.colors[word[-1]]

    def get_wordcloud(self):
        #alice_mask = np.array(Image.open("alice_mask.png"))

        wc = WordCloud(background_color="white", width=800, height=400, max_words=self.max_words)

        # generate word cloud
        wc.generate_from_frequencies(self.words)

        # color the wordclound
        wc.recolor(color_func=self.get_color_func)

        return wc
