package org.struy;


import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;

/**
 * @author with struy.
 * Create by 2022/6/29 17:03
 * email :yq1724555319@gmail.com
 */
@FunctionalInterface
interface FoldFunc<A, I> {
    /**
     * 窗口聚合期间的运算
     *
     * @param agg   聚合计算结果，在窗口内结果是一直累计的
     * @param input 输入事件
     * @return 每次聚合后更新聚合数据
     */
    A call(A agg, I input);
}

@FunctionalInterface
interface FireFunc<A, O> {
    /**
     * 窗口结束时的操作，返回窗口结束的输出
     *
     * @param agg 聚合计算的事件
     * @return
     */
    List<O> call(A agg);
}

/**
 * 窗口类,用于存储窗口数据
 *
 * @param <I>
 * @param <A>
 */
class Window<I, A> {
    private long start;
    private long end;
    private A aggrValue;

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public A getAggrValue() {
        return aggrValue;
    }

    public void setAggrValue(A aggrValue) {
        this.aggrValue = aggrValue;
    }

    public Window(long start, long end, A aggrValue) {
        this.start = start;
        this.end = end;
        this.aggrValue = aggrValue;
    }
}

/**
 * 事件封装
 *
 * @param <I>
 */
class Event<I> {
    private I event;
    // 从事件中抽取时间戳
    private long eventTime;

    public I getEvent() {
        return event;
    }

    public void setEvent(I event) {
        this.event = event;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }
}

/**
 * 窗口实现
 *
 * @param <I> 输入
 * @param <O> 输出
 * @param <A> 聚合(窗口期间的聚合)
 * @author struy
 */
public class WindowImpl<I, O, A> {
    /**
     * 窗口大小
     */
    private int windowSize;
    /**
     * 滑动大小
     */
    private int slidingSize;
    /**
     * 延迟关闭时间
     */
    private int delay;
    /**
     * 窗口内事件运算方法
     */
    private FoldFunc<A, I> fold;
    /**
     * 窗口结束事件输出
     */
    private FireFunc<A, O> fire;

    /**
     * 窗口缓存
     */
    private List<Window<I, A>> windows = new ArrayList<Window<I, A>>();
    /**
     * 水印时间，每秒生成
     */
    private long lastWatermark = 0;


    /**
     * 构造窗口
     *
     * @param windowSize  窗口大小, 单位毫秒
     * @param slidingSize 滑动窗口大小, 单位毫秒
     * @param delay       窗口延迟关闭的时间, 单位毫秒
     * @param fold        窗口内增量计算, (aggregationEvent: T, newEvent: T) => StreamingEvent[T]
     * @param fire        窗口结束后的处理
     */
    public WindowImpl(int windowSize, int slidingSize, int delay, FoldFunc<A, I> fold, FireFunc<A, O> fire) {

        this.windowSize = windowSize;
        this.slidingSize = slidingSize;
        this.delay = delay;
        this.fold = fold;
        this.fire = fire;
    }

    /**
     * 根据时间戳t，判断是否需要为这个事件新建若干窗口
     * 1. 计算事件所在的第一个窗口的开始时间firstWindowBegin
     * 2. 判断当前窗口集合是否为空
     * 2.1 如果是空，判断是否为滑动窗口
     * 2.1.1 如果是滑动窗口，那么从firstWindowBegin开始， 创建windowSize/sliding个窗口，每个窗口大小为windowSize, 开始时间间隔sliding
     * 2.1.2 如果是滚动窗口，那么直接创建一个窗口[firstWindowBegin, firstWindowBegin+windowSize)
     * 2.2 不为空, 判断是否为滑动窗口
     * 2.2.1 如果是滑动窗口，那么过滤出窗口集合中包含t的窗口，从最后一个符合条件的窗口的start+sliding开始创建滑动窗口
     * (如果没有符合条件的窗口，那么从firstWindowBegin开始)
     * 2.2.2 如果是滚动窗口，那么判断t是否在该窗口中。 如果不在， 那么新建一个窗口[firstWindowBegin, firstWindowBegin+windowSize)
     *
     * @param eventTime
     */
    private void createWindowIfNotExists(long eventTime) {
        long firstWindowBegin = calcFirstWindowBegin(eventTime);
        if (windows.isEmpty()) {
            if (slidingSize > 0) {
                long beginTime = firstWindowBegin;
                while (beginTime < eventTime) {
                    windows.add(new Window<>(beginTime, beginTime + windowSize, null));
                    // 滑动窗口
                    beginTime = beginTime + slidingSize;
                }
            } else {
                // 滚动窗口
                windows.add(new Window<>(firstWindowBegin, firstWindowBegin + windowSize, null));
            }
        } else {
            if (slidingSize > 0) {
                List<Window<I, A>> lastWindowOption = windows.stream().filter(window -> window.getEnd() >= eventTime).collect(Collectors.toList());
                long beginTime = firstWindowBegin;
                if (lastWindowOption.size() > 0) {
                    Window<I, A> lastWindow = lastWindowOption.get(lastWindowOption.size() - 1);
                    beginTime = lastWindow.getStart() + slidingSize;
                }
                while (beginTime < eventTime) {
                    windows.add(new Window<>(beginTime, beginTime + windowSize, null));
                    // 滑动窗口
                    beginTime = beginTime + slidingSize;
                }
            } else {
                // 滚动窗口
                Window<I, A> lastWindow = windows.get(windows.size() - 1);
                if (lastWindow.getEnd() < firstWindowBegin) {
                    windows.add(new Window<>(firstWindowBegin, firstWindowBegin + windowSize, null));
                }
            }
        }
    }

    /**
     * 计算时间戳所落在的第一个窗口的开始时间
     * 例如，window=60s，sliding=Some(10s)
     * 1. timestamp = 2019-10-14 08:55:43, 第一个窗口开始时间为08:54:50
     * 2. timestamp = 2019-10-14 08:55:55, 第一个窗口开始时间为08:55:00
     * 如果sliding=None, 相当于滚动窗口
     * 1. timestamp = 2019-10-14 08:55:43, 第一个窗口开始时间为08:55:00
     * 2. timestamp = 2019-10-14 08:55:55, 第一个窗口开始时间为08:55:00
     * 3. timestamp = 2019-10-14 08:56:08, 第一个窗口开始时间为08:56:00
     * <p>
     * 滑动窗口计算方式:
     * val time1 = timestamp - window
     * time1 - time1%sliding + sliding
     * 滚动窗口计算方式:
     * timestamp - timestamp%window
     *
     * @param eventTime
     * @return
     */
    private long calcFirstWindowBegin(long eventTime) {
        // 滚动窗口计算
        long lastWindowTime = eventTime - windowSize % windowSize;
        // 滑动窗口计算
        if (slidingSize > 0) {
            long _lastWindowTime = eventTime - windowSize;
            lastWindowTime = _lastWindowTime - _lastWindowTime % slidingSize + slidingSize;
        }
        return lastWindowTime;
    }

    /**
     * 关闭窗口时的操作
     */
    private void closeWindowsIfNecessary() {
        // 过期的窗口就可以关闭了
        List<Window<I, A>> closeWindows = windows.stream().filter(x -> x.getEnd() <= this.lastWatermark).collect(Collectors.toList());
        if (!closeWindows.isEmpty()) {
            closeWindows.forEach(window -> {
                if (null != window.getAggrValue()) {
                    fire.call(window.getAggrValue());
                }
            });
            // 窗口关闭移除
            windows.removeAll(closeWindows);
        }
    }


    /**
     * 模拟
     */
    public void startTest() {
        mockEvents();
        // receive 假装开始接收
        receive();
    }

    /**
     * 模拟事件消息
     */
    public void mockEvents() {
        new Thread(() -> {
            while (true) {
                Record record = new Record();
                record.setName("TEST-EVENT—" + new Random().nextInt(100));
                record.setTime(System.currentTimeMillis() - 10 * 1000);
                Event<Record> event = new Event<Record>();
                event.setEvent(record);
                event.setEventTime(record.getTime());
                // 模拟生产
                Data.eventStream.add(event);
                try {
                    // 每秒同步一次
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    /**
     * 模拟接收事件，事件是源源不断的
     */
    private void receive() {
        // 水印时间
        new Thread(() -> {
            while (true) {
//                System.out.println("假装同步时间");
                long currentTime = System.currentTimeMillis();
                this.lastWatermark = currentTime - delay;
                closeWindowsIfNecessary();
                try {
                    // 每秒同步一次
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        // 事件流
        new Thread(() -> {
            while (true) {
                // 模拟消费
                Event<I> event = Data.eventStream.poll();
                if (null != event) {
                    assert event != null;
                    System.out.println("假装我在接收事件:" + event.getEvent() + ":::" + event.getEventTime());
                    long timestamp = event.getEventTime();
                    if (timestamp < this.lastWatermark - windowSize) {
                        // 删除事件，因为已经过时了
                    } else {
                        // 新建窗口，如果需要的话
                        createWindowIfNotExists(event.getEventTime());
                        // 窗口内的聚合运算
                        windows.stream()
                                .filter(window -> window.getStart() <= timestamp && window.getEnd() > timestamp)
                                .forEach(window -> window.setAggrValue(this.fold.call(window.getAggrValue(), event.getEvent())));
                    }
                }
            }
        }).start();
    }


    public static void main(String[] args) {
        // 模拟窗口内聚合
        FoldFunc<List<Record>, Record> foldFunc = (List<Record> aggr, Record event) -> {
            if (null == aggr || aggr.isEmpty()) {
                aggr = new ArrayList<>();
                aggr.add(event);
            } else {
                aggr.add(event);
            }
            long timeMillis = System.currentTimeMillis();
            System.out.println(timeMillis + "===============聚合进行时" + "====" + aggr);
            return aggr;
        };
        // 模拟窗口关闭时的操作
        FireFunc<List<Record>, List<Record>> fireFunc = (List<Record> aggr) -> {
            ArrayList<List<Record>> res = new ArrayList<>();
            res.add(aggr);
            long timeMillis = System.currentTimeMillis();
            System.out.println(timeMillis + "===============窗口关闭了" + "====" + aggr);
            return res;
        };
        WindowImpl<Record, List<Record>, List<Record>> testWindow = new WindowImpl<Record, List<Record>, List<Record>>(30 * 1000, 5 * 1000, 2 * 1000, foldFunc, fireFunc);
        testWindow.startTest();
    }


}

/**
 * 模拟事件
 */
class Record {
    private String name;
    private long time;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "name=" + name + ",time=" + time;
    }
}

/**
 * 共享事件队列，用于模拟事件流信息
 */
class Data {
    /**
     * 模拟事件流
     */
    public static Queue<Event> eventStream = new ArrayBlockingQueue(1000);
}
