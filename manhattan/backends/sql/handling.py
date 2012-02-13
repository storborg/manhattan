from . import model
from .model import meta, timeseries, recent


max_recent_visitors = 500


def parse_timestamp(ts):
    return int(float(ts))


def handle_record(rec):
    assert rec.key in ('page', 'pixel', 'goal', 'split')

    if rec.key == 'page':
        record_page(ts=rec.timestamp,
                         vid=rec.vid,
                         site_id=rec.site_id,
                         ip=rec.ip,
                         method=rec.method,
                         url=rec.url,
                         user_agent=rec.user_agent,
                         referer=rec.referer)
    elif rec.key == 'pixel':
        record_pixel(ts=rec.timestamp,
                          vid=rec.vid,
                          site_id=rec.site_id)
    elif rec.key == 'goal':
        record_goal(ts=rec.timestamp,
                         vid=rec.vid,
                         site_id=rec.site_id,
                         name=rec.name,
                         value=rec.value,
                         value_type=rec.value_type,
                         value_format=rec.value_format)
    else:  # split
        record_split(ts=rec.timestamp,
                          vid=rec.vid,
                          site_id=rec.site_id,
                          name=rec.test_name,
                          selected=rec.selected)


def record_page(ts, vid, site_id, ip, method, url, user_agent, referer):
    ts = parse_timestamp(ts)
    vis = model.Visitor.find_or_create(visitor_id=vid,
                                       timestamp=ts)

    record_goal(ts, vid, site_id, 'viewed page', None, None, None)

    req = model.Request(visitor=vis,
                        timestamp=ts,
                        url=url,
                        ip=ip,
                        method=method)
    meta.Session.add(req)

    if recent.record_recent(ts, vid, ip):
        recent.truncate_recent(max_recent_visitors)

    meta.Session.flush()


def record_pixel(ts, vid, site_id):
    ts = parse_timestamp(ts)
    vis = model.Visitor.find_or_create(visitor_id=vid, timestamp=ts)
    vis.bot = False


def record_goal(ts, vid, site_id, name,
                value, value_type, value_format):
    ts = parse_timestamp(ts)

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


def record_split(ts, vid, site_id, name, selected):
    ts = parse_timestamp(ts)
    vis = model.Visitor.find_or_create(visitor_id=vid, timestamp=ts)
    test = model.Test.find_or_create(name=name)
    variant = model.Variant.find_or_create(test=test, name=selected)
    impr = model.Impression.find_or_create(visitor=vis, variant=variant)

    if impr.is_new:
        timeseries.record_impression(variant_id=variant.id,
                                     timestamp=ts)
