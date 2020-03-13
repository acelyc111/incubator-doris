#pragma once

#ifndef DORIS_BE_UDF_UDA_FUNNEL_INFO
#define DORIS_BE_UDF_UDA_FUNNEL_INFO

#include "udf/udf.h"
#include <iostream>
#include <list>
#include <set>
#include <string.h>
#include <unordered_set>
#include <vector>

#include "udf_ga/short_encode.h"

using namespace std;
extern int event_distinct_id;
namespace doris_udf {

#define MAX_EVENT_COUNT 5000
#define MAGIC_INT 32

static const char funnel_tag[] = "";
const int tag_size = 0;

long get_start_of_day(long ts);

struct Event {
    short _num;
    long _ts;
    int _event_distinct_id;
    Event(short num, long ts, int eventDistinctId)
        : _num(num), _ts(ts), _event_distinct_id(eventDistinctId) {}
    void init(short num, long ts, int eventDistinctId) {
        _num = num;
        _ts = ts;
        _event_distinct_id = eventDistinctId;
    }

    bool operator<(Event& other) {
        if (_ts < other._ts) {
            return true;
        } else if (_ts == other._ts) {
            if (_num >= other._num) {
                return false;
            } else if (_num < other._num) {
                return true;
            }
        } else {
            return false;
        }
        return false;
    }
};

struct FunnelInfoAgg {
    int old_ids[MAX_EVENT_COUNT];
    list<Event> _events;
    long _time_window;
    long _start_time;
    int _max_distinct_id;

    FunnelInfoAgg() {
        _time_window = 0L;
        _start_time = 0L;
        _max_distinct_id = 0;
    }
    void init() {
        _time_window = 0;
        _start_time = 0;
        _max_distinct_id = 0;
    }

    void add_event(Event* e) {
        _events.push_back(*e);
        if (e->_event_distinct_id > _max_distinct_id) {
            _max_distinct_id = e->_event_distinct_id;
        }
    }

    void add_event(short num, long ts, int event_distinct_id) {
        Event event(num, ts, event_distinct_id);
        _events.push_back(event);
        if (event_distinct_id > _max_distinct_id) {
            _max_distinct_id = event_distinct_id;
        }
    }

    list<Event> get_events() { return _events; }

    void trim(list<Event>& events, vector<Event>& rst);

    StringVal output(FunctionContext* context, StringVal* rst);
};

short make_value_f(int row, int column);

void funnel_info_init(FunctionContext* context, StringVal* funnelInfoAggVal);
void funnel_info_update(FunctionContext* context, BigIntVal& from_time, IntVal& time_window,
                        BigIntVal& steps, BigIntVal& event_time, StringVal* info_agg);
void funnel_info_update(FunctionContext* context, BigIntVal& from_time, BigIntVal& time_window,
                        BigIntVal& end_time1, BigIntVal& end_time2, BigIntVal& event_time,
                        StringVal& events, StringVal& event_name, StringVal* info_agg_val);
void funnel_info_merge(FunctionContext* context, const StringVal& src_agginfo_val,
                       StringVal* dest_agginfo_val);
StringVal funnel_info_finalize(FunctionContext* context, const StringVal& aggInfoVal);

FunnelInfoAgg parse(FunctionContext* context, const StringVal& aggInfoVal);
void parse(FunctionContext* context, const StringVal& aggInfoVal, FunnelInfoAgg& agg);
}

#endif
