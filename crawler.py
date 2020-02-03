import logging
import re
from urllib.parse import urlparse
from urllib.parse import urljoin
from lxml import etree
from bs4 import BeautifulSoup
from collections import Counter
from string import punctuation
from nltk.corpus import stopwords
import os
import pickle


def is_absolute(url):
    return bool(urlparse(url).netloc)  #https://stackoverflow.com/questions/8357098/how-can-i-check-if-a-url-is-absolute-using-python


logger = logging.getLogger(__name__)

#全局变量
downloaded_all_urls = []
subdomains = set()
mostoutlink_page = ['',0]
longest_page = ['',0]

traps = []

# def is_absolute(url):  # a helper function which checks if an url is absolute, credit to Lukáš Lalinský
#                        # https://stackoverflow.com/questions/8357098/how-can-i-check-if-a-url-is-absolute-using-python
#     return bool(urlparse(url).netloc)


class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """
    ANALYSIS_FILE_NAME = os.path.join(".", "analytics.txt")

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.url_counter = []
        self.cnt = Counter()
        self.subcnt = Counter()
        self.url_data = ''

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """

        while self.frontier.has_next_url():

            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)
            self.url_data = url_data

            for next_link in self.extract_next_links(url_data):

                next_link = next_link.strip('/')
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

        analysis_file = open(self.ANALYSIS_FILE_NAME, "wb")
        pickle.dump(self.cnt.most_common(50),analysis_file)
        pickle.dump(mostoutlink_page, analysis_file)
        pickle.dump(longest_page, analysis_file)
        pickle.dump(downloaded_all_urls, analysis_file)
        pickle.dump(subdomains, analysis_file)
        pickle.dump(traps, analysis_file)

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

        outputLinks = []  #https://stackoverflow.com/questions/24396406/find-most-common-words-from-a-website-in-python-3 credit to Padraic Cunningham

        doc = etree.HTML(url_data["content"])

        if doc:
            result = doc.xpath('//a/@href')
            ## get subdomain  https://stackoverflow.com/questions/6925825/get-subdomain-from-url-using-python

            for i in result:
                if is_absolute(i):
                    outputLinks.append(i)

                elif '@' in i:
                    continue

                elif len(i) > 1 and i[0] == '/':
                    parsed = urlparse(url_data["url"])
                    abs_url = "https://" + parsed.netloc + i
                    outputLinks.append(abs_url)

                elif len(i) > 1 and i[0] != '/' and i[0] == '.' and i[1] == '.':
                    abs_url = '/'.join(url_data["url"].split('/')[:-2]) + i[2:]
                    outputLinks.append(abs_url)

                elif len(i) > 1 and i[0] != '/' and i[0] != '.':
                    abs_url = '/'.join(url_data["url"].split('/')[:-1]) + '/' + i
                    outputLinks.append(abs_url)

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
        soup = BeautifulSoup(self.url_data["content"], 'lxml')
        word_count = 0

        parsed = urlparse(url)

        ##开始Invalid检测
        if parsed.scheme not in set(["http", "https"]):
            return False

        if len(self.url_counter) > 1 and self.url_counter[-0] == url:
            self.url_counter.append(url)
            return False

        else:
            if len(self.url_counter) > 3:
                for i in range(len(self.url_counter)):
                    traps.append(self.url_counter[i] + "\n\tTraps: Duplicate url appears")
                self.url_counter.clear()
                return False
            else:
                self.url_counter.clear()
                self.url_counter.append(url)

        if len(url.split("/")) > 9:
            traps.append(url + "\n\tTraps: Recursive paths detected")
            return False

        elif len(url.split('/')) != len(set(url.split('/'))):
            traps.append(url + "\n\tTraps: Repeat Directories detected")
            return False

        elif len(parsed.query.split("&")) > 3:
            traps.append(url + "\n\tTraps: Too many queries-may be dynamic page")
            return False

        #################
        #判断完成，证明这个valid之后的操作:
        self.subcnt[parsed.netloc.split(":")[0]] += 1
        # downloaded_all_urls.append(url)
        # # put all the words in this page into a counter
        # text = (''.join(s.findAll(text=True)) for s in soup.findAll('p'))
        # for words in text:
        #     word_count += len(words.split())
        #     for word in words.split():
        #         if word.rstrip(punctuation).lower() not in stopwords.words('english') and len(word) != 1:
        #             self.cnt[word] += 1
        #
        # # check if this page has most words
        # if word_count > longest_page[1]:
        #     longest_page[0] = url_data['url']
        #     longest_page[1] = word_count
        #################

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

