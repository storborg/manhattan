class Record(object):
    base_fields = ('timestamp', 'vid', 'site_id')
    fields = ()

    def __init__(self, **kwargs):
        for field in self.base_fields + self.fields:
            setattr(self, field, kwargs.get(field, ''))

    def to_list(self):
        return [self.key] + [getattr(self, field) for field in
                             self.base_fields + self.fields]

    @staticmethod
    def from_list(vals):
        cls = _record_types[vals[0]]
        kwargs = {field: val for field, val
                  in zip(cls.base_fields + cls.fields, vals[1:])}
        return cls(**kwargs)


class PageRecord(Record):
    key = 'page'
    fields = ('ip', 'method', 'url', 'user_agent', 'referer')


class PixelRecord(Record):
    key = 'pixel'
    fields = ()


class GoalRecord(Record):
    key = 'goal'
    fields = ('name', 'value', 'value_type', 'value_format')


class SplitRecord(Record):
    key = 'split'
    fields = ('test_name', 'selected')


_record_types = {cls.key: cls for cls in
                 (PageRecord, PixelRecord, GoalRecord, SplitRecord)}
