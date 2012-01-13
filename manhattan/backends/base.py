class Backend(object):

    def record_page(self, *args, **kwargs):
        raise NotImplementedError

    def record_pixel(self, *args, **kwargs):
        raise NotImplementedError

    def record_goal(self, *args, **kwargs):
        raise NotImplementedError

    def record_split(self, *args, **kwargs):
        raise NotImplementedError
