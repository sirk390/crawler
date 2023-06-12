from urllib.parse import urlparse


class Link():
    def __init__(self, linkstr, refered_at=None):
        self.linkstr = linkstr
        self.parsed = urlparse(linkstr) 
        self.netloc = self.parsed.netloc
        self.path = self.parsed.path
        self.refered_at = refered_at
        
    def __repr__(self):
        return self.linkstr
    
    def __hash__(self):
        return hash(self.linkstr)
    
    def __eq__(self, other):
        return self.linkstr == other.linkstr
    