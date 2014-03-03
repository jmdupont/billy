"""
    views specific to legislators
"""
import re
import json
import operator
import urllib2

from django.shortcuts import render, redirect
from django.http import Http404
from django.template.response import TemplateResponse
from django.views.decorators.csrf import ensure_csrf_cookie

from djpjax import pjax
import pymongo


from billy.utils import popularity
from billy.models import db, Metadata, DoesNotExist
from billy.core import settings as billy_settings

from .utils import templatename, mongo_fields


@pjax()
def legislators(request, abbr):
    '''
    Context:
        - metadata
        - chamber
        - chamber_title
        - chamber_select_template
        - chamber_select_collection
        - chamber_select_chambers
        - show_chamber_column
        - abbr
        - legislators
        - sort_order
        - sort_key
        - legislator_table
        - nav_active

    Templates:
        - billy/web/public/legislators.html
        - billy/web/public/chamber_select_form.html
        - billy/web/public/legislator_table.html
    '''
    try:
        meta = Metadata.get_object(abbr)
    except DoesNotExist:
        raise Http404

    spec = {'active': True, 'district': {'$exists': True}}

    chambers = dict((k, v['name']) for k, v in meta['chambers'].iteritems())

    chamber = request.GET.get('chamber', 'both')
    if chamber in chambers:
        spec['chamber'] = chamber
        chamber_title = meta['chambers'][chamber]['title'] + 's'
    else:
        chamber = 'both'
        chamber_title = 'Legislators'

    fields = mongo_fields('leg_id', 'full_name', 'photo_url', 'district',
                          'party', 'first_name', 'last_name', 'chamber',
                          billy_settings.LEVEL_FIELD, 'last_name')

    sort_key = 'district'
    sort_order = 1

    if request.GET:
        sort_key = request.GET.get('key', sort_key)
        sort_order = int(request.GET.get('order', sort_order))

    _legislators = meta.legislators(extra_spec=spec, fields=fields)

    def sort_by_district(obj):
        """Sort the legislators by district.
        """
        matchobj = re.search(r'\d+', obj.get('district', '') or '')
        if matchobj:
            return int(matchobj.group())
        else:
            return obj.get('district', '')

    _legislators = sorted(_legislators, key=sort_by_district)

    if sort_key != 'district':
        _legislators = sorted(
            _legislators,
            key=operator.itemgetter(sort_key),
            reverse=(sort_order == -1))
    else:
        _legislators = sorted(
            _legislators,
            key=sort_by_district,
            reverse=bool(0 > sort_order))

    sort_order = {1: -1, -1: 1}[sort_order]
    _legislators = list(_legislators)

    return TemplateResponse(
        request, templatename('legislators'),
        dict(metadata=meta, chamber=chamber,
             chamber_title=chamber_title,
             chamber_select_template=templatename('chamber_select_form'),
             chamber_select_collection='legislators',
             chamber_select_chambers=chambers, show_chamber_column=True,
             abbr=abbr,
             legislators=_legislators,
             sort_order=sort_order,
             sort_key=sort_key,
             legislator_table=templatename('legislator_table'),
             nav_active='legislators'))


@ensure_csrf_cookie
def legislator(request, abbr, _id, slug=None):
    '''
    TODO: slug not used
    Context:
        - feed_entry_template
        - vote_preview_row_template
        - roles
        - abbr
        - district_id
        - metadata
        - legislator
        - sources
        - sponsored_bills
        - legislator_votes
        - has_feed_entries
        - feed_entries
        - feed_entries_count
        - feed_entries_more_count
        - has_votes
        - nav_active

    Templates:
        - billy/web/public/legislator.html
        - billy/web/public/feed_entry.html
        - billy/web/public/vote_preview_row.html

    '''
    try:
        meta = Metadata.get_object(abbr)
    except DoesNotExist:
        raise Http404

    _legislator = db.legislators.find_one({'_id': _id})
    if _legislator is None:
        spec = {'_all_ids': _id}
        cursor = db.legislators.find(spec)
        msg = 'Two legislators returned for spec %r' % spec
        assert cursor.count() < 2, msg
        try:
            _legislator = cursor.next()
        except StopIteration:
            raise Http404('No legislator was found with leg_id = %r' % _id)
        else:
            return redirect(_legislator.get_absolute_url(), permanent=True)

    popularity.counter.inc('legislators', _id, abbr=abbr)

    if not _legislator['active']:
        return legislator_inactive(request, abbr, _legislator)

    district_id = None
    api_key = getattr(billy_settings, 'API_KEY', '')
    if api_key:
        qurl = "%sdistricts/%s/?apikey=%s" % (
            billy_settings.API_BASE_URL,
            abbr,
            billy_settings.API_KEY
        )
        try:
            data = urllib2.urlopen(qurl)
            districts = json.load(data)
            district_id = None
            for district in districts:
                legs = [x['leg_id'] for x in district['legislators']]
                if _legislator['leg_id'] in legs:
                    district_id = district['boundary_id']
                    break
        except urllib2.URLError:
            district_id = None

    sponsored_bills = _legislator.sponsored_bills(
        limit=6, sort=[('action_dates.first', pymongo.DESCENDING)])

    # Note to self: Another slow query
    legislator_votes = _legislator.votes_6_sorted()
    has_votes = bool(legislator_votes)
    feed_entries = _legislator.feed_entries()
    feed_entries_list = list(feed_entries.limit(5))
    return render(
        request, templatename('legislator'),
        dict(feed_entry_template=templatename('feed_entry'),
             vote_preview_row_template=templatename('vote_preview_row'),
             roles=_legislator.roles_manager,
             abbr=abbr,
             district_id=district_id,
             metadata=meta,
             legislator=_legislator,
             sources=_legislator['sources'],
             sponsored_bills=list(sponsored_bills),
             legislator_votes=list(legislator_votes),
             has_feed_entries=bool(feed_entries_list),
             feed_entries=feed_entries_list[:4],
             feed_entries_count=len(feed_entries_list),
             feed_entries_more_count=max([0, feed_entries.count() - 5]),
             has_votes=has_votes,
             nav_active='legislators'))


def legislator_inactive(request, abbr, _legislator):
    '''
    Context:
        - feed_entry_template
        - vote_preview_row_template
        - old_roles
        - abbr
        - metadata
        - legislator
        - sources
        - sponsored_bills
        - legislator_votes
        - has_votes
        - nav_active

    Templates:
        - billy/web/public/legislator.html
        - billy/web/public/feed_entry.html
        - billy/web/public/vote_preview_row.html
    '''
    sponsored_bills = _legislator.sponsored_bills(
        limit=6, sort=[('action_dates.first', pymongo.DESCENDING)])

    legislator_votes = list(_legislator.votes_6_sorted())
    has_votes = bool(legislator_votes)

    return render(
        request, templatename('legislator'),
        dict(feed_entry_template=templatename('feed_entry'),
             vote_preview_row_template=templatename('vote_preview_row'),
             old_roles=_legislator.old_roles_manager,
             abbr=abbr,
             metadata=_legislator.metadata,
             legislator=_legislator,
             sources=_legislator['sources'],
             sponsored_bills=list(sponsored_bills),
             legislator_votes=legislator_votes,
             has_votes=has_votes,
             nav_active='legislators'))
