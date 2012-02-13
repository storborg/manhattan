from sqlalchemy import create_engine

from manhattan import visitor

from . import model, handling
from .model import meta, timeseries


class SQLBackend(object):

    def __init__(self, sqlalchemy_url, pool_recycle=3600,
                 max_recent_visitors=500):
        self.engine = create_engine(sqlalchemy_url, pool_recycle=pool_recycle)
        handling.max_recent_visitors = max_recent_visitors
        model.init_model(self.engine)
        meta.metadata.create_all()

    def handle(self, rec):
        handling.handle_record(rec)

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

    def count(self, goal=None, variant=None, start=None, end=None):
        if goal:
            goal_id = self.get_goal(goal).id
        else:
            goal_id = None

        if variant:
            variant_id = self.get_variant(variant).id
        else:
            variant_id = None

        return timeseries.count(goal_id=goal_id, variant_id=variant_id,
                                start=start, end=end)

    def goal_value(self, goal, variant=None, start=None, end=None):
        goal = self.get_goal(goal)
        if variant:
            variant_id = self.get_variant(variant).id
        else:
            variant_id = None

        value = timeseries.total_value(goal_id=goal.id, variant_id=variant_id,
                                      start=start, end=end)

        assert goal.value_type in (visitor.SUM, visitor.AVERAGE, visitor.PER)

        if goal.value_type == visitor.SUM:
            return value

        elif goal.value_type == visitor.AVERAGE:
            num_conversions = timeseries.count(goal_id=goal.id,
                                               variant_id=variant_id,
                                               start=start, end=end)
            return value / num_conversions

        else:
            if variant_id:
                num_impressions = timeseries.count(variant_id=variant_id,
                                                   start=start, end=end)
            else:
                page_goal = self.get_goal(u'viewed page')
                num_impressions = timeseries.count(goal_id=page_goal.id,
                                                   start=start, end=end)
            return value / num_impressions

    def get_sessions(self, goal=None, variant=None):
        q = self._sessions_q(goal, variant)
        return [vid for vid, in q.all()]
