from collections import defaultdict
from sqlalchemy import create_engine

from . import model, handling, reporting
from .model import meta, resuming


class SQLBackend(object):

    def __init__(self, sqlalchemy_url, pool_recycle=3600,
                 max_recent_visitors=500):
        self._nonbot = set()
        self._nonbot_queue = defaultdict(list)

        self.engine = create_engine(sqlalchemy_url, pool_recycle=pool_recycle)
        handling.max_recent_visitors = max_recent_visitors
        model.init_model(self.engine)
        meta.metadata.create_all()

    def handle(self, rec, pointer):
        if rec.key == 'pixel':
            self.handle_nonbot(rec)
            self._nonbot.add(rec.vid)
            for queued_rec in self._nonbot_queue.pop(rec.vid, ()):
                self.handle_nonbot(queued_rec)

        elif rec.vid in self._nonbot:
            self.handle_nonbot(rec)

        else:
            self._nonbot_queue[rec.vid].append(rec)

        resuming.update_pointer(pointer)
        meta.Session.commit()

    def handle_nonbot(self, record):
        handling.handle_record(record)
        meta.Session.commit()

    def get_pointer(self):
        return resuming.get_pointer()

    def count(self, goal=None, variant=None, start=None, end=None):
        return reporting.count(goal=goal, variant=variant,
                               start=start, end=end)

    def goal_value(self, goal, variant=None, start=None, end=None):
        return reporting.goal_value(goal=goal, variant=variant,
                                    start=start, end=end)

    def get_sessions(self, goal=None, variant=None):
        q = reporting.sessions_q(goal, variant)
        return [vid for vid, in q.all()]
