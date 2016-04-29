from twisted.internet import defer

from moira.api.request import delayed
from moira.api.resources.redis import RedisResouce


class Targets(RedisResouce):

    def __init__(self, db):
        RedisResouce.__init__(self, db)

    def getChild(self, path, request):
        if not path:
            return self

    @delayed
    @defer.inlineCallbacks
    def render_GET(self, request):
        targets = yield self.db.getTargets()
        if 'search' in request.args.keys():
            targets_out = yield [t for t in targets if request.args['search'][0].encode("utf8") in t]
        else:
            targets_out = []
        self.write_json(request, {"list": targets_out[0:3]})
