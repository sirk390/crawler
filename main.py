import aiodns
import re
from collections import Counter
import urllib.request
import chardet
from multiprocessing import pool, Pool
import time
from heapq import heappush, heappop, heapify
from urllib.parse import urlparse
from htmlparser import MyHTMLParser, ExtractLinkParser
import traceback
import concurrent.futures
import aiohttp
import aiohttp.resolver
import os
import pickle
aiohttp.resolver.DefaultResolver = aiohttp.resolver.AsyncResolver

from _asyncio import get_event_loop
import asyncio
from asyncio.tasks import FIRST_COMPLETED
from aiohttp.client import ClientTimeout
from _collections import deque
from models import Link


class UrlDownloader():
    def __init__(self):
        self.session = aiohttp.ClientSession(timeout=ClientTimeout(total=30), headers={"User-Agent" :  'Mozilla/5.0'})
        self.loop = get_event_loop()

    async def download_and_parse(self, url):
        #with open(r"E:\tmp\logs\\" + url.replace("/", "_").replace(":", "_").replace("?", "_")[:200], "w", errors="ignore") as fout:
        try:
            response = await self.session.get(url)
            #fout.write(str(response.status))
            data = await response.text()
            #fout.write(data)
        except Exception as e:  #TimeoutError/UnicodeDecodeError
            #fout.write(traceback.format_exc())
            print (url, str(e))
            return ""
        else:
            #fout.write("success\n")
            if not data:
                print ("------------------------------------ none ------------------------------")
            return data


def extract_text(url, data):
    parser = MyHTMLParser(url)
    parser.feed(data)
    french_text = parser.french_text
    other_text = parser.other_text
    return (french_text, other_text)


class ElementWithPriority(object):
    def __init__(self, elm, priority):
        self.elm = elm
        self.priority = priority
    def __lt__(self, other):
        return (self.priority < other.priority)


class Processor():
    def __init__(self):
        self.crawled = set()
        self.bynetloc = {}
        self.netloccounter = Counter()
        self.text_bynetloc = Counter()
        self.fr_text_bynetloc = Counter()
        self.outfile = open("outfr", "w", errors="ignore")
        self.pool = concurrent.futures.ProcessPoolExecutor(5)
        self.loop = get_event_loop()
        self.nb = 0

    def get_priority(self, link):
        linkstr = link.linkstr.lower()

        if any(linkstr.endswith(ext) for ext in ['.png', '.jpg', '.pdf', '.png', '.zip']):
            return 100000
        pri = -1.0
        if link.netloc.endswith(".fr"):
            pri *= 2.0

        timesdone = self.netloccounter[link.netloc] # number of times we went of the site
        if timesdone < 30:
            pri *= 1.5
        elif timesdone >= 100:
            pri /= 3.0

        if self.text_bynetloc[link.netloc]:
            is_fr_domain = self.fr_text_bynetloc[link.netloc] / (self.text_bynetloc[link.netloc] + self.fr_text_bynetloc[link.netloc])
            pri *= (1.0 + is_fr_domain)
        #print (link.linkstr, pri)

        return pri

    async def done(self, link, data):
        #t1 = time.time()
        french_text, other_text = await self.loop.run_in_executor(self.pool, extract_text, link, data)
        #t2 = time.time()
        #print (link.linkstr, sum([len(t) for t in french_text]))
        #print (french_text)
        for fr in french_text:
            self.outfile.write(" " + fr)
        #t3 = time.time()
        #print (t2-t1, t3-t2)
        self.crawled.add(link)
        self.bynetloc[link.netloc] = link
        self.netloccounter[link.netloc] += 1
        self.text_bynetloc[link.netloc] += sum([len(t) for t in other_text])
        self.fr_text_bynetloc[link.netloc] += sum([len(t) for t in french_text])
        self.nb += 1

class Crawler():
    def __init__(self, processor=Processor()):
        self.linkqueue = []
        self.START_URLS = ['https://www.huffingtonpost.fr/',
                           "https://leparisien.fr",
                           "https://lemonde.fr/",
                           "https://new.google.fr/",
                           "https://liberation.fr/",
                           "https://mediapart.fr/",
                           ] #, 'https://news.google.fr'
        self.current_jobs = {}
        self.processor = processor
        self.loop = get_event_loop()
        self.i = 0
        self.max_linkqueue = 60000
        if os.path.exists("visited.db"):
            with open("visited.db", "rb") as fin:
                self.visited, self.linkqueue = pickle.load(fin)
        else:
            self.visited, self.linkqueue = set([]), []
        self.downloader = UrlDownloader()
        self.current_tasks = set()

    def reprioritize(self):
        self.linkqueue = [ElementWithPriority(l.elm, self.processor.get_priority(l.elm)) for l in self.linkqueue][:self.max_linkqueue]
        #with open("")
        heapify(self.linkqueue)

    async def process_url(self, url):
        self.visited.add(url)
        data = await self.downloader.download_and_parse(url)
        parser = ExtractLinkParser(url)
        parser.feed(data)
        for l in parser.links:
            if l.linkstr not in self.visited:
                # TODO remove # xxx to check for unicity
                self.visited.add(l.linkstr)
                priority = self.processor.get_priority(l)
                heappush(self.linkqueue, ElementWithPriority(l, priority))
        await self.processor.done(Link(url), data)

    async def run(self):

        for url in self.START_URLS:
            task = self.loop.create_task(self.process_url(url))
            self.current_tasks.add(task)

        while self.current_tasks:
            self.i += 1
            donelist, pending = await asyncio.wait(self.current_tasks, return_when=FIRST_COMPLETED)
            for task in donelist:
                try:
                    data = task.result()
                except Exception as e:
                    traceback.print_exc()
                    task.exception() # fetch exception
                #
                self.current_tasks.remove(task)
            while len(self.current_tasks) <= 2000 and self.linkqueue:
                elmpri = heappop(self.linkqueue)
                next_link = elmpri.elm
                print (next_link, elmpri.priority)
                task = self.loop.create_task(self.process_url(next_link.linkstr))
                self.current_tasks.add(task)
            if self.i % 1000 == 0:
                print (self.i, "current_jobs", len(self.current_tasks), "linkqueue:", len(self.linkqueue))
                self.reprioritize()
                with open("visited.db", "wb") as fout:
                    pickle.dump((self.visited, self.linkqueue), fout)

        await self.downloader.session.close()


if __name__ == "__main__":
    crawler = Crawler()

    loop = get_event_loop()
    loop.run_until_complete(crawler.run())