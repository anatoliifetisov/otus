from parsers.parser import Parser

from bs4 import BeautifulSoup
import mistune


class MarkdownParser(Parser):
    def parse(self, data, **kwargs):
        """
        Parses markdown and extracts links and their text
        :param data: markdown file
        :returns: a list of (links, text) tuples
        """
        html = mistune.markdown(data.text)
        soup = BeautifulSoup(html, "html.parser")

        if "level" in kwargs and kwargs["level"] == 0:
            links = []
            skip = 0
            for li in soup.find_all("li"):
                if li.p:
                    continue
                elif skip:
                    skip -= 1
                    continue
                elif li.ul:
                    sublist = li.find_all("a")
                    links += [(li.text.split("\n")[0], x.get("href")) for x in sublist]
                    skip += len(sublist)
                else:
                    links.append((li.a.string, li.a.get("href")))
        else:
            links = [(x.string, x.get("href")) for x in soup.find_all("a") if x.string]
        return links
