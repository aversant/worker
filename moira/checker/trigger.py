from twisted.python import log
from twisted.internet import defer
from datetime import datetime, timedelta
from time import time
from moira.graphite.evaluator import evaluateTarget
from moira.graphite import datalib
from moira.checker import state
from moira.checker import expression
from moira import config


class Trigger:

    def __init__(self, id, db):
        self.id = id
        self.db = db

    @defer.inlineCallbacks
    def init(self, now, fromTime=None):
        self.maintenance = False
        json, self.struct = yield self.db.getTrigger(self.id, tags=True)
        if json is None:
            defer.returnValue(False)
        for tag in self.struct["tags"]:
            tag_data = yield self.db.getTag(tag)
            if tag_data.get('maintenance'):
                self.maintenance = True
                break
        self.ttl = self.struct.get("ttl")
        self.ttl_state = self.struct.get("ttl_state", state.NODATA)
        self.last_check = yield self.db.getTriggerLastCheck(self.id)
        if self.last_check is None:
            self.last_check = {"metrics": {}, "state": state.NODATA, "timestamp": (fromTime or now) - 600}
        defer.returnValue(True)

    @defer.inlineCallbacks
    def get_timeseries(self, requestContext):
        targets = self.struct.get("targets", [])
        target_time_series = {}
        target_number = 1

        for target in targets:
            time_series = yield evaluateTarget(requestContext, target)

            if target_number > 1:
                if len(time_series) > 1:
                    raise "Target #%s has more than one timeseries" % target_number
                if len(time_series) == 0:
                    raise "Target #%s has no timeseries" % target_number

            for time_serie in time_series:
                time_serie.last_state = self.last_check["metrics"].get(
                                        time_serie.name, {
                                            "state": state.NODATA,
                                            "timestamp": time_serie.start})
            target_time_series["t%s" % target_number] = time_series
            target_number += 1

        defer.returnValue(target_time_series)

    @defer.inlineCallbacks
    def check(self, fromTime=None, now=None, cache_ttl=60):

        now = now or int(time())

        log.msg("Checking trigger %s" % self.id)
        initialized = yield self.init(now, fromTime=fromTime)
        if not initialized:
            raise StopIteration

        if fromTime is None:
            fromTime = self.last_check.get("timestamp", now)

        requestContext = datalib.createRequestContext(str(fromTime - 600), str(now))

        check = {"metrics": {}, "state": state.OK, "timestamp": now}
        try:
            time_series = yield self.get_timeseries(requestContext)

            for metric in requestContext['metrics']:
                yield self.db.cleanupMetricValues(metric, now - config.METRICS_TTL,
                                                  cache_key=metric, cache_ttl=cache_ttl)

            if len(time_series) == 0:
                if self.ttl:
                    check["state"] = self.struct.get("ttl_state", state.NODATA)
                    check["msg"] = "Trigger has no metrics"
                    yield self.compare_state(check, self.last_check, now)
            else:

                for t1 in time_series["t1"]:

                    check["metrics"][t1.name] = metric_state = t1.last_state.copy()

                    for value_timestamp in xrange(t1.start, now + t1.step, t1.step):

                        if value_timestamp <= t1.last_state["timestamp"]:
                            continue

                        if self.ttl and value_timestamp + self.ttl < self.last_check["timestamp"]:
                            log.msg("Metric %s TTL expired with timestamp %s" %
                                    (t1.name, value_timestamp))
                            metric_state["state"] = self.struct.get("ttl_state", state.NODATA)
                            metric_state["timestamp"] = value_timestamp + self.ttl
                            if "value" in metric_state:
                                del metric_state["value"]
                            yield self.compare_state(metric_state,
                                                     t1.last_state,
                                                     value_timestamp + self.ttl, value=None,
                                                     metric=t1.name)
                            continue

                        expression_values = {}

                        for target_number in xrange(1, len(time_series) + 1):
                            target_name = "t%s" % target_number
                            tN = time_series[target_name][0] if target_number > 1 else t1
                            value_index = (value_timestamp - tN.start) / tN.step
                            tN_value = tN[value_index] if len(tN) > value_index else None
                            expression_values[target_name] = tN_value
                            if tN_value is None:
                                break

                        t1_value = expression_values["t1"]

                        if None in expression_values.values():
                            continue

                        expression_values.update({'warn_value': self.struct.get('warn_value'),
                                                  'error_value': self.struct.get('error_value'),
                                                  'PREV_STATE': metric_state['state']})

                        metric_state["state"] = expression.getExpression(self.struct.get('expression'),
                                                                         **expression_values)
                        metric_state["value"] = t1_value
                        metric_state["timestamp"] = value_timestamp
                        yield self.compare_state(metric_state, t1.last_state,
                                                 value_timestamp, value=t1_value,
                                                 metric=t1.name)

                    if self.ttl and metric_state["timestamp"] + self.ttl < self.last_check["timestamp"]:
                        log.msg("Metric %s TTL expired for state %s" % (t1.name, metric_state))
                        metric_state["state"] = self.struct.get("ttl_state", state.NODATA)
                        metric_state["timestamp"] += self.ttl
                        if "value" in metric_state:
                            del metric_state["value"]
                        yield self.compare_state(metric_state, t1.last_state, metric_state["timestamp"], metric=t1.name)
        except StopIteration:
            raise
        except:
            log.err()
            check["state"] = state.EXCEPTION
            check["msg"] = "Trigger evaluation exception"
            yield self.compare_state(check, self.last_check, now)
        yield self.db.setTriggerLastCheck(self.id, check)

    @defer.inlineCallbacks
    def compare_state(self,
                      current_state,
                      last_state,
                      timestamp,
                      value=None,
                      metric=None):
        current_state_value = current_state["state"]
        last_state_value = last_state["state"]
        if current_state_value != last_state_value or \
                (last_state.get("suppressed") and current_state_value != state.OK):
            event = {
                "trigger_id": self.id,
                "state": current_state_value,
                "old_state": last_state_value,
                "timestamp": timestamp,
                "metric": metric
            }
            current_state["event_timestamp"] = timestamp
            if value is not None:
                event["value"] = value
            if self.isSchedAllows(timestamp):
                if not self.maintenance:
                    log.msg("Writing new event: %s" % event)
                    yield self.db.pushEvent(event)
                    current_state["suppressed"] = False
                else:
                    current_state["suppressed"] = True
                    log.msg("Event %s suppressed due maintenance" % str(event))
            else:
                current_state["suppressed"] = True
                log.msg("Event %s suppressed due trigger schedule" % str(event))
        last_state["state"] = current_state_value

    def isSchedAllows(self, ts):
        sched = self.struct.get('sched')
        if sched is None:
            return True

        timestamp = ts - ts % 60 - sched["tzOffset"] * 60
        date = datetime.fromtimestamp(timestamp)
        if not sched['days'][date.weekday()]['enabled']:
            return False
        day_start = datetime.fromtimestamp(timestamp - timestamp % (24 * 3600))
        start_datetime = day_start + timedelta(minutes=sched["startOffset"])
        end_datetime = day_start + timedelta(minutes=sched["endOffset"])
        if date < start_datetime:
            return False
        if date > end_datetime:
            return False
        return True
