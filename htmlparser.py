import collections
from language_identify import open_ngramstats, match_specific_language
from html.parser import HTMLParser
from models import Link
from urllib.parse import urljoin
import traceback

NGRAMSTAT = open_ngramstats("3gram-stats.dat", 3)

def match_french(data):
    return match_specific_language(data, NGRAMSTAT, "fr")

class MyHTMLParser(HTMLParser):
    def __init__(self, url):
        super().__init__()
        self.url = url
        self.reset()
        self.data = []
        self.opentags = collections.Counter()

        self.links = set()
        self.french_text = []
        self.other_text = []
        self.total_text_size = 0

    def handle_data(self, d):
    # identify language
        str_d = d.strip()
        self.total_text_size += len(str_d)
        if not str_d:
            return
        try:
            fr_match = match_specific_language(str_d, NGRAMSTAT, "fr")
            #fr_match = 0.0
        except Exception as e:
            fr_match = 0.0
        if len(str_d) > 200:
            if fr_match > 0.2: 
                self.french_text.append(str_d)
            else:
                self.other_text.append(str_d)

    def get_data(self):
        return ''.join(self.data)

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            if "href" in d:
                try:
                    link = Link(urljoin(self.url, d["href"]), self.url)
                except:
                    pass
                else:
                    self.links.add(link)
        self.opentags[tag] += 1

    def handle_endtag(self, tag):
        self.opentags[tag] -= 1
        
        




class ExtractLinkParser(HTMLParser):
    def __init__(self, url):
        super().__init__()
        self.links = set()
        self.url = url
        
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            if "href" in d and d["href"] and not d["href"].startswith("mailto:"):
                try:
                    link = Link(urljoin(self.url, d["href"]), self.url)
                except:
                    pass
                else:
                    self.links.add(link)