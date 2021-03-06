import os
#import time
import logging
#import datetime
import importlib
import json
#import sys
#import traceback
import scrapelib
from billy.scrape.validator import DatetimeValidator
_log = logging.getLogger("billy")
from billy.core import settings
from billy.utils import JSONEncoderPlus

#import scrapelib


class ScrapeError(Exception):
    """
    Base class for scrape errors.
    """
    def __init__(self, msg, orig_exception=None):
        self.msg = msg
        self.orig_exception = orig_exception

    def __str__(self):
        if self.orig_exception:
            return '%s\nOriginal Exception: %s' % (self.msg,
                                                   self.orig_exception)
        else:
            return self.msg


class NoDataForPeriod(ScrapeError):
    """
    Exception to be raised when no data exists for a given period
    """
    def __init__(self, period):
        self.period = period

    def __str__(self):
        return 'No data exists for %s' % self.period


class Scraper(scrapelib.Scraper):
    """ Base class for all Scrapers

    Provides several useful methods for retrieving URLs and checking
    arguments against metadata.
    """

    latest_only = False

    """
    Filter only this bill id
    """

    def set_filter_bill_id(self, billid):
#        _log.debug("old self.filter_bill_id was :%s" % self.filter_bill_id)
        self.filter_bill_id = billid
#        _log.debug("net self.filter_bill_id= %s" % self.filter_bill_id)

    def get_filter_bill_id(self):
#        _log.debug("self.filter_bill_id %s" % self.filter_bill_id)
        return self.filter_bill_id

    def __init__(self, metadata, output_dir=None, strict_validation=None,
                 fastmode=False):
        """
        Create a new Scraper instance.

        :param metadata: metadata for this scraper
        :param output_dir: the data directory to use
        :param strict_validation: exit immediately if validation fails
        """
        super(Scraper, self).__init__()

        # scrapelib overrides
        self.timeout = settings.SCRAPELIB_TIMEOUT
        self.cache_storage = scrapelib.FileCache(settings.BILLY_CACHE_DIR)
        self.requests_per_minute = settings.SCRAPELIB_RPM
        self.retry_attempts = settings.SCRAPELIB_RETRY_ATTEMPTS
        self.retry_wait_seconds = settings.SCRAPELIB_RETRY_WAIT_SECONDS

        if fastmode:
            self.requests_per_minute = 0
            self.cache_write_only = False

        self.filter_bill_id = False
        self.metadata = metadata
        self.output_dir = output_dir
        self.output_names = set()

        # make output_dir
        if self.output_dir is None:
            _log.debug("output_dir is none")
            raise Exception("output_dir missing")

        _log.debug("output_dir %s" % self.output_dir)
        print ("output_dir: %s" % self.output_dir)
        os.path.isdir(self.output_dir) or os.path.makedirs(self.output_dir)

        # validation
        self.strict_validation = strict_validation
        self.validator = DatetimeValidator()
        self._schema = {}
        self._load_schemas()

        self.follow_robots = False

        # logging convenience methods
        self.logger = logging.getLogger("billy")
        self.log = self.logger.info
        self.info = self.logger.info
        self.debug = self.logger.debug
        self.warning = self.logger.warning
        self.error = self.logger.error
        self.critical = self.logger.critical

    def _load_schemas(self):
        """ load all schemas into schema dict """

        types = ('bill', 'committee', 'person', 'vote', 'event', 'speech')

        for _type in types:
            schema_path = os.path.join(os.path.split(__file__)[0],
                                       '../schemas/%s.json' % _type)
            self._schema[_type] = json.load(open(schema_path))
            self._schema[_type]['properties'][settings.LEVEL_FIELD] = {
                'minLength': 2, 'type': 'string'}

        # bills & votes
        self._schema['bill']['properties']['session']['enum'] = \
            self.all_sessions()
        self._schema['vote']['properties']['session']['enum'] = \
            self.all_sessions()

        # legislators
        terms = [t['name'] for t in self.metadata['terms']]
        # ugly break here b/c this line is nearly impossible to split
        self._schema['person']['properties']['roles'][
            'items']['properties']['term']['enum'] = terms

    @property
    def object_count(self):
        # number of distinct output filenames
        return len(self.output_names)

    def validate_json(self, obj):
        try:
            self.validator.validate(obj, self._schema[obj['_type']])
        except ValueError as ve:
            _log.debug("Validation Failed!")
            _log.debug("For schema : %s" % self._schema)
            _log.debug("Object : %s" % obj)
            _log.debug(ve)

            self.warning(str(ve))
            if self.strict_validation:
                raise ve

    def all_sessions(self):
        _log.debug("all sessions")
        sessions = []

        if 'terms' not in self.metadata:  # we expect a metadata and terms
            return sessions

        metadata_terms = self.metadata['terms']

        _log.debug(metadata_terms)

        for t in metadata_terms:

            _log.debug("t : %s" % t)
            _log.debug("t sessions: %s" % t['sessions'])
            sessions.extend(t['sessions'])
        return sessions

    def validate_session(self, session, latest_only=False):
        """ Check that a session is present in the metadata dictionary.

        raises :exc:`~billy.scrape.NoDataForPeriod` if session is invalid

        :param session:  string representing session to check
        """
        if latest_only:
            if session != self.metadata['terms'][-1]['sessions'][-1]:
                raise NoDataForPeriod(session)

        for t in self.metadata['terms']:
            if session in t['sessions']:
                return True
        raise NoDataForPeriod(session)

    def validate_term(self, term, latest_only=False):
        """ Check that a term is present in the metadata dictionary.

        raises :exc:`~billy.scrape.NoDataForPeriod` if term is invalid

        :param term:        string representing term to check
        :param latest_only: if True, will raise exception if term is not
                            the current term (default: False)
        """

        if latest_only:
            if term == self.metadata['terms'][-1]['name']:
                return True
            else:
                _log.debug("term:%s" % term)
                _log.debug("does not equal name :%s" %
                           self.metadata['terms'][-1]['name'])
                _log.debug("metadata:")
                _log.debug(self.metadata)

                raise NoDataForPeriod(term)

        for t in self.metadata['terms']:
            if term == t['name']:
                return True
        raise NoDataForPeriod(term)

    def save_object(self, obj):
        self.log('save %s %s', obj['_type'], unicode(obj))

        # copy jurisdiction to LEVEL_FIELD
        obj[settings.LEVEL_FIELD] = getattr(self, 'jurisdiction')

        filename = obj.get_filename()
        self.output_names.add(filename)     # keep tally of all output names

        # pluralize type
        if obj['_type'] == 'speech':
            data_dir = 'speeches'
        elif obj['_type'] == 'person':
            data_dir = 'legislators'
        else:
            data_dir = obj['_type'] + 's'

        with open(os.path.join(self.output_dir, data_dir, filename),
                  'w') as f:
            json.dump(obj, f, cls=JSONEncoderPlus)

        # validate after writing, allows for inspection
        self.validate_json(obj)


class SourcedObject(dict):
    """ Base object used for data storage.

    Base class for :class:`~billy.scrape.bills.Bill`,
    :class:`~billy.scrape.legislators.Legislator`,
    :class:`~billy.scrape.votes.Vote`,
    and :class:`~billy.scrape.committees.Committee`.

    SourcedObjects work like a dictionary.  It is possible
    to add extra data beyond the required fields by assigning to the
    `SourcedObject` instance like a dictionary.
    """

    def __init__(self, _type, **kwargs):
        super(SourcedObject, self).__init__()
        self['_type'] = _type
        self['sources'] = []
        self.update(kwargs)

    def add_source(self, url, **kwargs):
        """
        Add a source URL from which data related to this object was scraped.

        :param url: the location of the source
        """
        self['sources'].append(dict(url=url, **kwargs))


def get_scraper(mod_path, scraper_type):
    """ import a scraper from the scraper registry """

    # act of importing puts it into the registry
    try:
        module = importlib.import_module(mod_path)
    except ImportError as e:
        raise ScrapeError("could not import %s" % mod_path, e)

    # now find the class within the module
    ScraperClass = None

    for k, v in module.__dict__.iteritems():
        if k.startswith('_'):
            continue
        if getattr(v, 'scraper_type', None) == scraper_type:
            if ScraperClass:
                raise ScrapeError("two %s scrapers found in module %s: %s %s" %
                                  (scraper_type, mod_path, ScraperClass, k))
            ScraperClass = v

    if not ScraperClass:
        raise ScrapeError("no %s scraper found in module %s" % (
            scraper_type, mod_path))

    return ScraperClass


def check_sessions(metadata, sessions):
    all_sessions_in_terms = list(reduce(lambda x, y: x + y,
                                        [x['sessions'] for x
                                         in metadata['terms']]))
    # copy the list to avoid modifying it
    metadata_session_details = list(metadata.get('_ignored_scraped_sessions',
                                                 []))

    _log.debug("all_sessions_in_terms:%s" % all_sessions_in_terms)

    for k, v in metadata['session_details'].iteritems():
        try:
            _log.debug("check session details from meta data:%s" % k)
            all_sessions_in_terms.remove(k)
        except ValueError:
            raise ScrapeError('session %s exists in session_details but not '
                              'in a term' % k)

        metadata_session_details.append(v.get('_scraped_name'))

    if not sessions:
        raise ScrapeError('no sessions from session_list()')

    _log.debug("all_sessions_in_terms:%s" % all_sessions_in_terms)

    if all_sessions_in_terms:
        raise ScrapeError('no session_details for session(s): %r' %
                          all_sessions_in_terms)

    unaccounted_sessions = []

    _log.debug("metadata_session_details:%s" % metadata_session_details)

    for s in sessions:
        if s not in metadata_session_details:
            _log.debug("unaccounted for")
            _log.debug(s)
            unaccounted_sessions.append(s)

    for s in unaccounted_sessions:
        if len(s) > 0:
            _log.debug("unaccounted for")
            _log.debug(s)
            raise ScrapeError('session(s) unaccounted for: %r' %
                              unaccounted_sessions)
