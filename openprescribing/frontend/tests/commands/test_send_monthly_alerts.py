# -*- coding: utf-8 -*-
import re

from mock import patch
from mock import ANY

from django.test import TestCase
from frontend.models import Measure
from frontend.management.commands.send_monthly_org_alerts import Command


class AnyStringWith(str):
    def __eq__(self, other):
        return self in other


class GetBookmarksTestCase(TestCase):
    fixtures = ['bookmark_alerts']

    def test_get_bookmarks_without_options(self):
        bookmarks = Command().get_bookmarks()
        active = all([x.user.is_active for x in bookmarks])
        self.assertEqual(len(bookmarks), 3)
        self.assertTrue(active)

    def test_get_bookmarks_with_options(self):
        bookmarks = Command().get_bookmarks(
            recipient_email='s@s.com',
            ccg='03V',
            practice='P87629'
        )
        self.assertEqual(len(bookmarks), 1)
        self.assertEqual(bookmarks[0].user.email, 's@s.com')
        self.assertTrue(bookmarks[0].user.profile.key)
        self.assertTrue(bookmarks[0].user.id)
        self.assertEqual(bookmarks[0].pct.code, '03V')
        self.assertEqual(bookmarks[0].practice.code, 'P87629')


@patch('frontend.views.bookmark_utils.InterestingMeasureFinder')
@patch('frontend.views.bookmark_utils.EmailMultiAlternatives')
@patch('frontend.views.bookmark_utils.attach_image')
class SendEmailTestCase(TestCase):
    fixtures = ['bookmark_alerts', 'measures']

    def test_email_recipient(self, attach_image, email, finder):
        test_context = _makeContext()
        finder.return_value.context_for_org_email.return_value = test_context
        Command().handle(recipient_email='s@s.com',
                         ccg='03V',
                         practice='P87629')
        email.assert_any_call(
            ANY,  # subject
            ANY,  # body
            "hello@openprescribing.net",
            ['s@s.com']
        )
        email.return_value.send.assert_any_call()

    def test_email_body_no_data(self, attach_image, email, finder):
        test_context = _makeContext()
        finder.return_value.context_for_org_email.return_value = test_context
        Command().handle(recipient_email='s@s.com',
                         ccg='03V',
                         practice='P87629')
        attachment = email.return_value.attach_alternative
        # Name of the practice
        attachment.assert_called_once_with(
            AnyStringWith('1/ST ANDREWS MEDICAL PRACTICE'), 'text/html')
        # Unsubscribe link
        attachment.assert_called_once_with(
            AnyStringWith('/bookmarks/dummykey'), 'text/html')

        attachment.assert_called_once_with(
            AnyStringWith("We've no new information"), 'text/html')

    def test_email_body_declines(self, attach_image, email, finder):
        measure = Measure.objects.get(pk='cerazette')
        test_context = _makeContext(declines=[(measure, 99.92, 0.12, 10.002)])
        finder.return_value.context_for_org_email.return_value = test_context
        attach_image.return_value = 'unique-image-id'
        Command().handle(recipient_email='s@s.com',
                         ccg='03V',
                         practice='P87629')
        attachment = email.return_value.attach_alternative
        attachment.assert_called_once_with(
            AnyStringWith("This practice slipped"), 'text/html')

        body = attachment.call_args[0][0]
        self.assertRegexpMatches(
            body, 'slipped by 100 centiles .* on '
            '<a href="/practice/P87629/measures/cerazette/".*>'
            'Cerazette vs. Desogestrel</a>')
        self.assertIn('<img src="cid:unique-image-id', body)
        self.assertNotIn("Your best prescribing areas", body)
        self.assertNotIn("Cost savings", body)

    def test_email_body_two_declines(self, attach_image, email, finder):
        measure = Measure.objects.get(pk='cerazette')
        test_context = _makeContext(declines=[
            (measure, 99.92, 0.12, 10.002),
            (measure, 30, 10, 0)

        ])
        finder.return_value.context_for_org_email.return_value = test_context
        attach_image.return_value = 'unique-image-id'
        Command().handle(recipient_email='s@s.com',
                         ccg='03V',
                         practice='P87629')
        attachment = email.return_value.attach_alternative
        body = attachment.call_args[0][0]
        self.assertRegexpMatches(
            body, 'It also slipped by 20 centiles')

    def test_email_body_three_declines(self, attach_image, email, finder):
        measure = Measure.objects.get(pk='cerazette')
        test_context = _makeContext(declines=[
            (measure, 99.92, 0.12, 10.002),
            (measure, 30, 10, 0),
            (measure, 20, 10, 0)
        ])
        finder.return_value.context_for_org_email.return_value = test_context
        attach_image.return_value = 'unique-image-id'
        Command().handle(recipient_email='s@s.com',
                         ccg='03V',
                         practice='P87629')
        attachment = email.return_value.attach_alternative
        body = attachment.call_args[0][0]
        self.assertRegexpMatches(
            body, 'It also slipped on the following measures')
        self.assertRegexpMatches(
            body, re.compile('<ul.*<li>By 20 centiles on.*'
                             '<li>By 10 centiles on.*</ul>', re.DOTALL))

    def test_email_body_worst(self, attach_image, email, finder):
        measure = Measure.objects.get(pk='cerazette')
        test_context = _makeContext(worst=[measure])
        finder.return_value.context_for_org_email.return_value = test_context
        attach_image.return_value = 'unique-image-id'
        Command().handle(recipient_email='s@s.com',
                         ccg='03V',
                         practice='P87629')
        attachment = email.return_value.attach_alternative
        attachment.assert_called_once_with(
            AnyStringWith("We've found"), 'text/html')

        body = attachment.call_args[0][0]
        self.assertRegexpMatches(
            body, re.compile(
                'the lowest decile on.*<a href="/practice/P87629'
                '/measures/cerazette/".*>'
                "Cerazette vs. Desogestrel</a>", re.DOTALL))
        self.assertIn('<img src="cid:unique-image-id', body)

    def test_email_body_three_worst(self, attach_image, email, finder):
        measure = Measure.objects.get(pk='cerazette')
        test_context = _makeContext(worst=[measure, measure, measure])
        finder.return_value.context_for_org_email.return_value = test_context
        attach_image.return_value = 'unique-image-id'
        Command().handle(recipient_email='s@s.com',
                         ccg='03V',
                         practice='P87629')
        attachment = email.return_value.attach_alternative
        body = attachment.call_args[0][0]
        self.assertRegexpMatches(
            body, 'It was also in the lowest decile on the following measures')
        self.assertRegexpMatches(
            body, re.compile('<ul.*<li>.*Desogestrel.*'
                             '<li>.*Desogestrel.*</ul>', re.DOTALL))

    def test_email_body_two_savings(self, attach_image, email, finder):
        measure = Measure.objects.get(pk='cerazette')
        test_context = _makeContext(possible_savings=[
            (measure, 9.9), (measure, 1.12)])
        finder.return_value.context_for_org_email.return_value = test_context
        attach_image.return_value = 'unique-image-id'
        Command().handle(recipient_email='s@s.com',
                         ccg='03V',
                         practice='P87629')
        attachment = email.return_value.attach_alternative
        body = attachment.call_args[0][0]
        self.assertIn(
            "These add up to £11 of potential savings".decode('utf-8'),
            body)
        self.assertRegexpMatches(
            body, '<li.*>£10 on <a href="/practice/P87629'
            '/measures/cerazette/".*>'
            "Cerazette vs. Desogestrel</a>".decode('utf-8'))

    def test_email_body_one_saving(self, attach_image, email, finder):
        measure = Measure.objects.get(pk='cerazette')
        test_context = _makeContext(possible_savings=[
            (measure, 9.9)])
        finder.return_value.context_for_org_email.return_value = test_context
        Command().handle(recipient_email='s@s.com',
                         ccg='03V',
                         practice='P87629')
        attachment = email.return_value.attach_alternative
        body = attachment.call_args[0][0]
        self.assertIn(
            "If it had prescribed in line with the average practice",
            body)
        self.assertRegexpMatches(
            body, 'it could have saved £10 on <a href="/practice/P87629'
            '/measures/cerazette/".*>'
            "Cerazette vs. Desogestrel</a>".decode('utf-8'))

    def test_email_body_total_savings(self, attach_image, email, finder):
        test_context = _makeContext(possible_top_savings_total=9000.1)
        finder.return_value.context_for_org_email.return_value = test_context
        Command().handle(recipient_email='s@s.com',
                         ccg='03V',
                         practice='P87629')
        attachment = email.return_value.attach_alternative
        body = attachment.call_args[0][0]
        self.assertIn(
            "it could save around <b>£9,000</b>".decode('utf-8'),
            body)

def _makeContext(**kwargs):
    empty_context = {
        'most_changing': {
            'declines': [
            ],
            'improvements': [
            ]
        },
        'top_savings': {
            'possible_top_savings_total': 0.0,
            'possible_savings': [],
            'achieved_savings': []
        },
        'worst': [
        ],
        'best': [
        ]
    }
    if 'declines' in kwargs:
        empty_context['most_changing']['declines'] = kwargs['declines']
    if 'improvements' in kwargs:
        empty_context['most_changing']['improvements'] = (
            kwargs['improvements'])
    if 'possible_top_savings_total' in kwargs:
        empty_context['top_savings']['possible_top_savings_total'] = (
            kwargs['possible_top_savings_total'])
    if 'possible_savings' in kwargs:
        empty_context['top_savings']['possible_savings'] = (
            kwargs['possible_savings'])
    if 'achieved_savings' in kwargs:
        empty_context['top_savings']['achieved_savings'] = (
            kwargs['achieved_savings'])
    if 'worst' in kwargs:
        empty_context['worst'] = kwargs['worst']
    if 'best' in kwargs:
        empty_context['best'] = kwargs['best']
    return empty_context
