## 路径分析UDAF
1. 函数参数--事件名称, 时间戳, 切分session时长, 起始/终止事件, 决定起始/终止, 最大层数
            exmaple: ["xiangkan", 157000, 60000, load, 0, 10]
            
2. 函数返回值--A&B&C;2-A&B&C&D&A;1-A&D;1 (路径A-->B-->C两次, A-->B-->C-->D-->A一次, A-->D一次)

3. 代码执行步骤:
    
    3.1 根据时间戳对用户行为进行排序
    
    3.2 根据传入的切分session时长参数切分session
    
    3.3 根据决定起始/终止标志位决定起始/终止事件
    
    3.4 遍历用户事件,寻找包含起始/终止事件的session,并根据最大层数进行截断
    
    3.5 统计用户路径并返回
    
4. Example:
    
    4.1 用户的访问行为为[ABCBCDABCABCCBDAABC]
    
    4.2 根据切分时长切分session,结果为[ABC | BC | DABCABC | CBDA | ABC]
    
    4.3 寻找以A事件为起点, 最大层数为10的session路径[ABC | ABCABC | A | ABC]
    
    4.4 统计结果返回A&B&C;2-A&B&C&A&B&C;1-A;1


## 漏斗分析UDAF
1. 函数参数说明:
 - from_time: 查询的开始时间
 - time_window: 漏斗窗口期，在该窗口期内发生的漏斗才视为有效漏斗
 - steps: 漏斗步骤，根据bit编码，例如第3步为100, 十进制表示为4, 因为漏斗中可能有相同的事件属于不同的步骤，例如漏斗步骤可能为click->open->open->end
 - event_time: 传入的事件发生时间

2. 代码中数据结构说明:

```
struct Event {
    short _num; //事件所属步骤
    long _ts; //事件的时间戳
    int _event_distinct_id; //事件的唯一标识，用于防止当相同事件属于不同步骤时误算入漏斗
}

struct FunnelInfoAgg {
    int old_ids[MAX_EVENT_COUNT];   // 判断事件是否已计算过， 结合_event_distinct_id用于防止当相同事件属于不同步骤时误算入漏斗
    list<Event> _events;    // 用一用户所有事件的集合
    long _time_window;  //漏斗窗口期
    int _max_distinct_id; //该用户最大的事件id
}

// 指针中存放的数据结构类型及含义
aggInfo->ptr=[from_time(int64)]->[time_window(int64)]->{[step(int16)][event_time(int64)][event_distinct_id(int32)], [step(int16)][event_time(int64)][event_distinct_id(int32)]...}

```

3. Parse函数
将上述aggInfo->ptr数据转为FunnelInfoAgg类型

4. Output函数(漏斗处理步骤)
 - 对FunnelInfoAgg中用户事件进行排序
 - 过滤重复的事件，提升运行效率(例如AAAAAAA的事件发生顺序只保留开始与结束AA),如果漏斗顺序为AAAA如何处理?
 - 对处理后的总事件进行计算，获取该用户的漏斗

5. Notice
 - MAGIC_INT，用于在update中获取该事件的步骤
 - old_ids[MAX_ENENT_COUNT]与event_distinct_id，用于标记事件是否使用过，解决如下问题，若一个漏斗为BAAC, 不添加该处理，会导致BAC也算入该BAAC漏斗中
 - trim作用, 某些用户存在大量重复事件，例如AAAAA，不做过滤会导致每次从第一个A为窗口开始计算，冗余计算影响漏斗性能，因此进行过滤，降低时间复杂度
 - MAX_EVENT_COUNT=5000, 一个用户的记录超过5000条时视为异常用户，为避免异常用户的计算拖慢漏斗性能，将异常的用户进行过滤
    
