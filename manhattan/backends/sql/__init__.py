from sqlalchemy import create_engine

from manhattan import visitor

from . import model
from .model import meta, timeseries, recent


class SQLBackend(object):

    def __init__(self, sqlalchemy_url, pool_recycle=3600,
                 max_recent_visitors=500):
        self.engine = create_engine(sqlalchemy_url, pool_recycle=pool_recycle)
        self.max_recent_visitors = max_recent_visitors
        model.init_model(self.engine)
        meta.metadata.create_all()

    def parse_timestamp(self, ts):
        return int(float(ts))

    def handle(self, rec):
        assert rec.key in ('page', 'pixel', 'goal', 'split')

        if rec.key == 'page':
            self.record_page(ts=rec.timestamp,
                             vid=rec.vid,
                             site_id=rec.site_id,
                             ip=rec.ip,
                             method=rec.method,
                             url=rec.url,
                             user_agent=rec.user_agent,
                             referer=rec.referer)
        elif rec.key == 'pixel':
            self.record_pixel(ts=rec.timestamp,
                              vid=rec.vid,
                              site_id=rec.site_id)
        elif rec.key == 'goal':
            self.record_goal(ts=rec.timestamp,
                             vid=rec.vid,
                             site_id=rec.site_id,
                             name=rec.name,
                             value=rec.value,
                             value_type=rec.value_type,
                             value_format=rec.value_format)
        else:  # split
            self.record_split(ts=rec.timestamp,
                              vid=rec.vid,
                              site_id=rec.site_id,
                              name=rec.test_name,
                              selected=rec.selected)

        meta.Session.commit()

    def record_page(self, ts, vid, site_id, ip, method, url, user_agent,
                    referer):
        ts = self.parse_timestamp(ts)
        vis = model.Visitor.find_or_create(visitor_id=vid,
                                           timestamp=ts)

        self.record_goal(ts, vid, site_id, 'viewed page', None, None, None)

        req = model.Request(visitor=vis,
                            timestamp=ts,
                            url=url,
                            ip=ip,
                            method=method)
        meta.Session.add(req)

        if recent.record_recent(ts, vid, ip):
            recent.truncate_recent(self.max_recent_visitors)

        meta.Session.flush()

    def record_pixel(self, ts, vid, site_id):
        ts = self.parse_timestamp(ts)
        vis = model.Visitor.find_or_create(visitor_id=vid, timestamp=ts)
        vis.bot = False

    def record_goal(self, ts, vid, site_id, name,
                    value, value_type, value_format):
        ts = self.parse_timestamp(ts)

        value = float(value) if value else None

        vis = model.Visitor.find_or_create(visitor_id=vid, timestamp=ts)
        goal = model.Goal.find_or_create(name=name,
                                         value_type=value_type,
                                         value_format=value_format)

        assert goal.value_type == value_type, (
            "can't change value type from %r to %r" %
            (goal.value_type, value_type))
        assert goal.value_format == value_format, (
            "can't change value format from %r to %r" %
            (goal.value_format, value_format))

        conv = model.Conversion.find_or_create(visitor=vis,
                                               goal=goal,
                                               value=value)

        if conv.is_new:
            timeseries.record_conversion(goal_id=goal.id,
                                         timestamp=ts,
                                         value=value)

        variants = meta.Session.query(model.Variant).\
                join(model.Variant.impressions).\
                filter_by(visitor=vis)

        for variant in variants:
            vc = model.VariantConversion.find_or_create(goal=goal,
                                                        visitor=vis,
                                                        variant=variant)
            if vc.is_new:
                timeseries.record_variant_conversion(goal_id=goal.id,
                                                     variant_id=variant.id,
                                                     timestamp=ts,
                                                     value=value)

    def record_split(self, ts, vid, site_id, name, selected):
        ts = self.parse_timestamp(ts)
        vis = model.Visitor.find_or_create(visitor_id=vid, timestamp=ts)
        test = model.Test.find_or_create(name=name)
        variant = model.Variant.find_or_create(test=test, name=selected)
        impr = model.Impression.find_or_create(visitor=vis, variant=variant)

        if impr.is_new:
            timeseries.record_impression(variant_id=variant.id,
                                         timestamp=ts)

    def get_goal(self, name):
        return meta.Session.query(model.Goal).filter_by(name=name).one()

    def get_variant(self, variant):
        test_name, pop_name = variant
        test = meta.Session.query(model.Test).\
                filter_by(name=test_name).one()

        return meta.Session.query(model.Variant).\
                filter_by(test=test, name=pop_name).one()

    def _sessions_q(self, goal=None, variant=None):
        q = meta.Session.query(model.Visitor.visitor_id).filter_by(bot=False)

        if goal:
            goal = self.get_goal(goal)

        if variant:
            variant = self.get_variant(variant)

        if goal and variant:
            # Use VariantConversion table.
            q = q.join(model.VariantConversion,
                       model.VariantConversion.visitor_id ==
                       model.Visitor.visitor_id).\
                    filter(model.VariantConversion.goal == goal).\
                    filter(model.VariantConversion.variant == variant)

        elif goal:
            # Use Conversion table.
            q = q.join(model.Conversion,
                       model.Conversion.visitor_id ==
                       model.Visitor.visitor_id).\
                    filter(model.Conversion.goal == goal)
        elif variant:
            # Use Impression table.
            q = q.join(model.Impression,
                       model.Impression.visitor_id ==
                       model.Visitor.visitor_id).\
                    filter(model.Impression.variant == variant)
        return q

    def count(self, goal=None, variant=None):
        if goal:
            goal_id = self.get_goal(goal).id
        else:
            goal_id = None

        if variant:
            variant_id = self.get_variant(variant).id
        else:
            variant_id = None

        return timeseries.count(goal_id=goal_id, variant_id=variant_id)

    def goal_value(self, goal, variant=None):
        goal = self.get_goal(goal)
        if variant:
            variant_id = self.get_variant(variant).id
        else:
            variant_id = None

        value = timeseries.total_value(goal_id=goal.id, variant_id=variant_id)

        assert goal.value_type in (visitor.SUM, visitor.AVERAGE, visitor.PER)

        if goal.value_type == visitor.SUM:
            return value

        elif goal.value_type == visitor.AVERAGE:
            num_conversions = timeseries.count(goal_id=goal.id,
                                               variant_id=variant_id)
            return value / num_conversions

        else:
            if variant_id:
                num_impressions = timeseries.count(variant_id=variant_id)
            else:
                page_goal = self.get_goal(u'viewed page')
                num_impressions = timeseries.count(goal_id=page_goal.id)
            return value / num_impressions

    def get_sessions(self, goal=None, variant=None):
        q = self._sessions_q(goal, variant)
        return [vid for vid, in q.all()]
