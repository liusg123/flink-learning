package com.zhisheng.libraries.cep;

import com.zhisheng.libraries.event.LoginEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: 刘守纲
 * @Date: 2020/8/20 0:41
 */
public class LoginFailMain {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        SingleOutputStreamOperator ds = env.readTextFile("").map(m -> {
            String[] e = m.split("");
            long l = Long.parseLong(e[1]);
            return new LoginEvent(e[0], l, "ip", e[2]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(30L)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getFailTime();
            }
        })
                .keyBy(k -> k.getUserId()).process(new LoginFailProc<String, LoginEvent, LoginWaring>(4));
    }
}

class LoginFailProc<S, L, L1> extends KeyedProcessFunction<String, LoginEvent, LoginWaring> {
    private int maxFailTimes = 0;

    ListState<LoginEvent> loginFailState = getRuntimeContext().getListState(new ListStateDescriptor<>("loginWarning", LoginEvent.class));

    public LoginFailProc(final int maxFailTimes) {
        this.maxFailTimes = maxFailTimes;
    }

    @Override
    public void processElement(LoginEvent event, Context ctx, Collector<LoginWaring> out) throws Exception {
        if ("fail".equals(event.getEventType())) {
            if (loginFailState.get().iterator().hasNext()) {
                ctx.timerService().registerEventTimeTimer(event.getFailTime() * 1000 + 2000L);
            }
            loginFailState.add(event);
        } else {
            loginFailState.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginWaring> out) throws Exception {
        List<LoginEvent> allLoginFails = new ArrayList();
        Iterator<LoginEvent> it = loginFailState.get().iterator();
        while (it.hasNext()) {
            allLoginFails.add(it.next());
        }
        if (allLoginFails.size() >= maxFailTimes) {
            out.collect(new LoginWaring(ctx.getCurrentKey()));
        }
        loginFailState.clear();
    }
}

class LoginWaring {

    public LoginWaring(String currentKey) {
    }
}