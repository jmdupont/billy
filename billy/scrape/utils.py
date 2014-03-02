import subprocess
#import sys
#from lxml import etree
#import traceback
import scrapelib
import lxml.html


class NoData(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class NoDoc(NoData):

    def __init__(self, value):
        self.value = value


class NoXpath(NoData):

    def __init__(self, value):
        self.value = value


def url_xpath(url, path):

    data = scrapelib.urlopen(url)

    if (data is None):
        print "Could not get data from url:%s" % (url)
        raise NoData(url)

    doc = lxml.html.fromstring(data)

    if (doc is None):
        print "Could not decode XML Doc:%s" % (data)
        raise NoDoc(data)

#    print etree.tostring(doc)
#    print doc
#    print "Check  path:%s Doc:%s" % (path,doc)

    result = doc.xpath(path)

#    print result
#    print len(result)

    if (result is None):
#        print doc
#        print doc.tag
#        print etree.tostring(doc)
#        print "Xpath failed path:%s Doc:%s" % (path,doc)
        print "Xpath failed"
        raise NoXpath(data)

#    print "url_xpath %s" % result
#    exc_type, exc_value, exc_traceback = sys.exc_info()
#    traceback.print_exc()
#    traceback.print_tb(exc_traceback, file=sys.stdout)
    return result


def convert_pdf(filename, type='xml'):
    commands = {'text': ['pdftotext', '-layout', filename, '-'],
                'text-nolayout': ['pdftotext', filename, '-'],
                'xml':  ['pdftohtml', '-xml', '-stdout', filename],
                'html': ['pdftohtml', '-stdout', filename]}
    try:
        pipe = subprocess.Popen(commands[type], stdout=subprocess.PIPE,
                                close_fds=True).stdout
    except OSError:
        raise EnvironmentError("error running %s, missing executable?" %
                               ' '.join(commands[type]))
    data = pipe.read()
    pipe.close()
    return data


def pdf_to_lxml(filename, type='html'):
    import lxml.html
    text = convert_pdf(filename, type)
    return lxml.html.fromstring(text)
