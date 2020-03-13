//
// Created by wangcong on 2019/9/18.
//

#ifndef DORIS_PATH_ANALYSIS_COMMON_H
#define DORIS_PATH_ANALYSIS_COMMON_H

#include <algorithm>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/priority_queue.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

namespace doris_udf {

struct Event;
class EventComparator;
typedef std::priority_queue<Event, std::vector<Event>, EventComparator> event_queue;

const char eventConnector('&');
const char timesConnector(':');
const char separator(';');
const std::string prefixSeparator("%");
const int defaultLevel(3);

struct Event {
    long _ts;
    std::string _eventName;

    Event() : _ts(0), _eventName("") {}

    Event(long ts, std::string eventName) : _ts(ts), _eventName(std::move(eventName)) {}

    void reset() {
        this->_ts = 0;
        this->_eventName = "";
    }

    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar& _ts;
        ar& _eventName;
    }
};

class EventComparator {
public:
    EventComparator() : _isReverse(false) {}
    explicit EventComparator(bool isReverse) : _isReverse(isReverse) {}

    bool operator()(const Event& lhs, const Event& rhs) {
        if (_isReverse) {
            return lhs._ts < rhs._ts;
        }
        return lhs._ts > rhs._ts;
    }

    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar& _isReverse;
    }

private:
    bool _isReverse;
};

class EventInfoAgg {
public:
    EventInfoAgg()
        : _initialized(false), _sessionInterval(0), _targetEventName(""), _isReverse(false),
          _level(defaultLevel), _events(event_queue(EventComparator(_isReverse))) {}

    void addEvent(long ts, std::string eventName) { _events.push(Event(ts, std::move(eventName))); }

    bool initialized() const { return this->_initialized; }

    void merge(const EventInfoAgg& other) {
        if (!this->_initialized) {
            this->initialize(other.getSessionInterval(), other.getTargetEventName(),
                             other.getIsReverse(), other.getLevel());
            this->_initialized = true;
        }
        auto otherEvents = other.getEvents();
        if (otherEvents.empty()) {
            return;
        }
        while (!otherEvents.empty()) {
            this->_events.push(otherEvents.top());
            otherEvents.pop();
        }
    }

    void initialize(int sessionInterval, const std::string& targetEventName, bool isReverse,
                    int level) {
        this->_sessionInterval = sessionInterval;
        this->_targetEventName = targetEventName;
        this->_isReverse = isReverse;
        this->_level = level;
        this->_initialized = true;
    }

    void reset() {
        _initialized = false;
        _sessionInterval = 0;
        _targetEventName = "";
        _isReverse = false;
        _level = 0;
        _events = event_queue();
    }

    bool getInitialized() const { return this->_initialized; }

    int getSessionInterval() const { return this->_sessionInterval; }

    std::string getTargetEventName() const { return this->_targetEventName; }

    bool getIsReverse() const { return this->_isReverse; }

    int getLevel() const { return this->_level; }

    event_queue getEvents() const { return this->_events; }

    std::string output() {
        std::vector<Event> sortedEvents(this->_events.size());
        int count = 0;
        while (!this->_events.empty()) {
            sortedEvents[count++] = this->_events.top();
            this->_events.pop();
        }

        std::vector<std::vector<std::string>> partitionedEvents;
        std::vector<std::string> tmpEvents;
        Event firstEvent;
        Event secondEvent;
        // 对事件切session并分组
        for (size_t i = 0; i < sortedEvents.size(); ++i) {
            firstEvent = sortedEvents[i];
            if (i < sortedEvents.size() - 1) {
                secondEvent = sortedEvents[i + 1];
            } else {
                secondEvent.reset();
            }

            tmpEvents.emplace_back(firstEvent._eventName);
            // 切分session
            if (secondEvent._ts) {
                if ((secondEvent._ts - firstEvent._ts) >= this->_sessionInterval) {
                    partitionedEvents.emplace_back(tmpEvents);
                    tmpEvents.clear();
                }
            } else {
                if (!tmpEvents.empty()) {
                    partitionedEvents.emplace_back(tmpEvents);
                }
            }
        }

        std::vector<std::string> results;
        std::unordered_map<std::string, int> resultCounts;
        for (const auto& eventsName : partitionedEvents) {
            auto validEvent = std::end(eventsName);
            for (auto it = eventsName.begin(); it != eventsName.end(); ++it) {
                std::vector<std::string> tmp_vec;
                boost::split(tmp_vec, *it, boost::is_any_of(prefixSeparator));
                if (tmp_vec.empty()) {
                    break;
                }
                if (tmp_vec[0] == _targetEventName) {
                    validEvent = it;
                    break;
                }
            }
            if (validEvent == std::end(eventsName)) {
                continue;
            }
            std::string result;
            for (size_t i = 0; i < _level; ++i) {
                if (validEvent != std::end(eventsName)) {
                    result += (*validEvent++);
                    result += eventConnector;
                    result += eventConnector;
                }
            }
            while (result.back() == eventConnector) {
                result.pop_back();
            }
            results.emplace_back(result);
        }
        for (const auto& result : results) {
            resultCounts[result]++;
        }
        // 输出形式为A&&B&&C:2;B&&C&&D:1
        std::string outputs;
        for (const auto& result : resultCounts) {
            outputs += result.first;
            outputs += timesConnector;
            outputs += std::to_string(result.second);
            outputs += separator;
        }
        if (outputs.back() == separator) {
            outputs.pop_back();
        }
        return outputs;
    }

    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar& _initialized;
        ar& _sessionInterval;
        ar& _targetEventName;
        ar& _isReverse;
        ar& _level;
        ar& _events;
    }

private:
    friend class boost::serialization::access;
    bool _initialized;     // 初始化标志位
    int _sessionInterval;  // 划分session的间隔
    std::string _targetEventName;
    bool _isReverse;  // 开始事件为false, 结束事件为true
    int _level;       // 最多的展示路径
    event_queue _events;
};

inline std::string StringValToStdString(const StringVal& input) {
    return (input.ptr == nullptr) ? "" : std::string(reinterpret_cast<char*>(input.ptr), input.len);
}

}  // namespace doris_udf

#endif  // DORIS_PATH_ANALYSIS_COMMON_H
