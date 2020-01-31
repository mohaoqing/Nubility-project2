import logging
import re
from urllib.parse import urlparse
from urllib.parse import urljoin
from lxml import etree, html
from bs4 import BeautifulSoup
from collections import Counter
from string import punctuation
from nltk.corpus import stopwords
import requests
from html.parser import HTMLParser


def is_absolute(url):
    return bool(urlparse(url).netloc) #https://stackoverflow.com/questions/8357098/how-can-i-check-if-a-url-is-absolute-using-python


logger = logging.getLogger(__name__)

#全局变量
downloaded_all_urls = set()
subdomains = set()
mostoutlink_page = ['',0]
longest_page = ['',0]
cnt = Counter()

# def is_absolute(url):  # a helper function which checks if an url is absolute, credit to Lukáš Lalinský
#                        # https://stackoverflow.com/questions/8357098/how-can-i-check-if-a-url-is-absolute-using-python
#     return bool(urlparse(url).netloc)


class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

        print(cnt.most_common(50))
        print(mostoutlink_page)
        print(longest_page)
        print("################### Downloaded URLS:")
        for i in downloaded_all_urls:
            print("  ", i)

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        if url_data["http_code"] == 404:
            return []
        outputLinks = []
        word_count = 0
        soup = BeautifulSoup(url_data["content"], 'lxml')

        #https://stackoverflow.com/questions/24396406/find-most-common-words-from-a-website-in-python-3 credit to Padraic Cunningham


        # put all the words in this page into a counter
        text = (''.join(s.findAll(text=True)) for s in soup.findAll('p'))
        for words in text:
            word_count += len(words.split())
            for word in words.split():
                if word.rstrip(punctuation).lower() not in stopwords.words('english') and len(word) != 1:
                    cnt[word] += 1

        #check if this page has most words
        if word_count > longest_page[1]:
            longest_page[0] = url_data['url']
            longest_page[1] = word_count

        doc = etree.HTML(url_data["content"])

        if doc:
            result = doc.xpath('//a/@href')

            ## get subdomain  https://stackoverflow.com/questions/6925825/get-subdomain-from-url-using-python
            for i in result:
                if is_absolute(i):
                    downloaded_all_urls.add(i)
                    outputLinks.append(i)
                    subdomains.add(urlparse(i).hostname.split('.')[0])
                elif len(i) > 1 and i[0] == '/':
                    abs_url = urljoin(url_data["url"], i[1:])
                    downloaded_all_urls.add(abs_url)
                    outputLinks.append(abs_url)
                    subdomains.add(urlparse(abs_url).hostname.split('.')[0])

        ## check if this page has most out links
        if (len(outputLinks)) > mostoutlink_page[1]:
            mostoutlink_page[0] = url_data['url']
            mostoutlink_page[1] = len(outputLinks)

        ##多重实验先决条件， comment out 来验证个例
        return outputLinks




    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False
