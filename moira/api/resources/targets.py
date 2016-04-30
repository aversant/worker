from twisted.internet import defer
from twisted.web import http

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
        if 'name' not in request.args.keys():
            request.setResponseCode(http.BAD_REQUEST)
            request.finish()
        else:
            targets_out = yield [t for t in targets if request.args['name'][0].encode("utf8") in t]
            self.write_json(request, {"list": targets_out[0:3]})
