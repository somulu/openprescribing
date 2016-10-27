# -*- coding: utf-8 -*-
from django.core.management.base import BaseCommand
from frontend.models import OrgBookmark
from frontend.models import User
from frontend.models import Profile

from frontend.views import bookmark_utils


class Command(BaseCommand):
    args = ''
    help = ''' Send monthly emails based on bookmarks. With no arguments, sends
    an email to every user for each of their bookmarks, for the
    current month. With arguments, sends a test email to the specified
    user for the specified organisation.'''

    def add_arguments(self, parser):
        parser.add_argument('--recipient-email')
        parser.add_argument('--ccg')
        parser.add_argument('--practice')

    def get_bookmarks(self, **options):
        if 'recipient_email' in options:
            dummy_user = User(email=options['recipient_email'], id='dummyid')
            dummy_user.profile = Profile(key='dummykey')
            bookmarks = [OrgBookmark(
                user=dummy_user,
                pct_id=options['ccg'],
                practice_id=options['practice']
            )]
        else:
            # XXX add a constraint here to ensure we don't send two
            # emails for one month.
            bookmarks = OrgBookmark.objects.filter(
                user__is_active=True)
        return bookmarks

    def handle(self, *args, **options):
        # First, generate the images for each email
        for org_bookmark in self.get_bookmarks(**options):
            stats = bookmark_utils.InterestingMeasureFinder(
                practice=org_bookmark.practice or options['practice'],
                pct=org_bookmark.pct or options['ccg']).context_for_org_email()
            recipient_id = org_bookmark.user.id
            msg = bookmark_utils.make_email_html(
                org_bookmark.user, org_bookmark, stats)
            # Optional Anymail extensions:
            msg.metadata = {"user_id": recipient_id,
                            "experiment_variation": 1}
            msg.tags = ["monthly_update"]
            msg.track_clicks = True
            msg.esp_extra = {"sender_domain": "openprescribing.net"}
            msg.send()
