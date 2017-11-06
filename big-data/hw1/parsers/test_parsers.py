import unittest
from collections import namedtuple

from parsers.markdown_parser import MarkdownParser

class TestMarkdownParser(unittest.TestCase):
    Wrapper = namedtuple("Wrapper", "text")

    test_data = Wrapper(
        "# Awesome Awesomeness\n\n" +
        "A curated list of amazingly awesome awesomeness.\n\n" +
        "- Programming Languages\n\n" +
        "\t- [C/C++](https://github.com/fffaraz/awesome-cpp)\n" +
        "\t- [CMake](https://github.com/onqtam/awesome-cmake)\n" +
        "\t- Clojure\n" +
        "\t\t- [by @mbuczko](https://github.com/mbuczko/awesome-clojure)\n" +
        "\t\t- [by @razum2um](https://github.com/razum2um/awesome-clojure)\n"
    )

    def test_parse_top_level(self):
        parser = MarkdownParser(None)
        parsed_data = parser.parse(self.test_data, level=0)
        self.assertEquals(len(parsed_data), 4)
        self.assertListEqual([("C/C++", "https://github.com/fffaraz/awesome-cpp"),
                              ("CMake", "https://github.com/onqtam/awesome-cmake"),
                              ("Clojure", "https://github.com/mbuczko/awesome-clojure"),
                              ("Clojure", "https://github.com/razum2um/awesome-clojure")],
                             parsed_data)

    def test_parse_general_case(self):
        parser = MarkdownParser(None)
        parsed_data = parser.parse(self.test_data)
        self.assertEquals(len(parsed_data), 4)
        self.assertListEqual([("C/C++", "https://github.com/fffaraz/awesome-cpp"),
                              ("CMake", "https://github.com/onqtam/awesome-cmake"),
                              ("by @mbuczko", "https://github.com/mbuczko/awesome-clojure"),
                              ("by @razum2um", "https://github.com/razum2um/awesome-clojure")],
                             parsed_data)


if __name__ == '__main__':
    unittest.main()
