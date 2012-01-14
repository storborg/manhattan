from sqlalchemy import create_engine

from manhattan.backends.base import Backend

from . import model
from .model import meta


class SQLBackend(Backend):

    def __init__(self, sqlalchemy_url):
        self.engine = create_engine(sqlalchemy_url)
        model.init_model(self.engine)
        meta.metadata.create_all()

    def record_page(self, ts, vid, url):
        vis = model.Visitor.find_or_create(visitor_id=vid)
        vis.timestamp = ts

        self.record_goal(ts, vid, 'viewed page', None)

        req = model.Request(visitor=vis,
                            timestamp=ts,
                            url=url)
        meta.Session.add(req)

        meta.Session.commit()

    def record_pixel(self, ts, vid):
        vis = model.Visitor.find_or_create(visitor_id=vid)
        vis.bot = False
        meta.Session.commit()

    def record_goal(self, ts, vid, name, value):
        vis = model.Visitor.find_or_create(visitor_id=vid)
        goal = model.Goal.find_or_create(name=name,
                                         value_type=None,
                                         value_format=None)
        model.Conversion.find_or_create(visitor=vis, goal=goal)

        variants = meta.Session.query(model.Variant).\
                join(model.Variant.impressions).\
                filter_by(visitor=vis)

        for variant in variants:
            model.VariantConversion.find_or_create(goal=goal,
                                                   visitor=vis,
                                                   variant=variant)

        meta.Session.commit()

    def record_split(self, ts, vid, name, selected):
        vis = model.Visitor.find_or_create(visitor_id=vid)
        test = model.Test.find_or_create(name=name)
        variant = model.Variant.find_or_create(test=test, name=selected)
        model.Impression.find_or_create(visitor=vis, variant=variant)
        meta.Session.commit()

    def _sessions_q(self, goal=None, variant=None):
        q = meta.Session.query(model.Visitor.visitor_id).filter_by(bot=False)

        if goal:
            goal = meta.Session.query(model.Goal).filter_by(name=goal).one()
            q = q.join(model.Conversion,
                       model.Conversion.visitor_id ==
                       model.Visitor.visitor_id).\
                    filter(model.Conversion.goal == goal)

        if variant:
            test_name, pop_name = variant
            test = meta.Session.query(model.Test).\
                    filter_by(name=test_name).one()

            variant = meta.Session.query(model.Variant).\
                    filter_by(test=test, name=pop_name).one()

            q = q.join(model.Impression,
                       model.Impression.visitor_id ==
                       model.Visitor.visitor_id).\
                    filter(model.Impression.variant == variant)

        return q

    def count(self, goal, variant=None):
        q = self._sessions_q(goal, variant)
        return q.count()

    def get_sessions(self, goal=None, variant=None):
        q = self._sessions_q(goal, variant)
        return [vid for vid, in q.all()]
