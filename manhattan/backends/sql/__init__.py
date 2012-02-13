from sqlalchemy import create_engine

from . import model, handling, reporting
from .model import meta


class SQLBackend(object):

    def __init__(self, sqlalchemy_url, pool_recycle=3600,
                 max_recent_visitors=500):
        self.engine = create_engine(sqlalchemy_url, pool_recycle=pool_recycle)
        handling.max_recent_visitors = max_recent_visitors
        model.init_model(self.engine)
        meta.metadata.create_all()

    def handle(self, rec):
        handling.handle_record(rec)

    def count(self, goal=None, variant=None, start=None, end=None):
        return reporting.count(goal=goal, variant=variant,
                               start=start, end=end)

    def goal_value(self, goal, variant=None, start=None, end=None):
        return reporting.goal_value(goal=goal, variant=variant,
                                    start=start, end=end)

    def get_sessions(self, goal=None, variant=None):
        q = reporting.sessions_q(goal, variant)
        return [vid for vid, in q.all()]
