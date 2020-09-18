package com.zhisheng.template;


import com.zhisheng.template.pojo.UserBehavior;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class Main {

    private static WatermarkGeneratorSupplier.Context ws;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.readTextFile("D:\\project\\myfile.csv").map(m -> {
            String[] dataArrs = m.split(",");
            return new UserBehavior(dataArrs[0], dataArrs[1], DateUtils.parseDate(dataArrs[2],"yyyy-MM-dd"));
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<UserBehavior>() {
            private long currentTimestamp = Long.MIN_VALUE;
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                Watermark watermark = new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 5000);
                return watermark;
            }

            @Override
            public long extractTimestamp(UserBehavior ub, long l) {
                currentTimestamp = Math.max(ub.getTs().getTime(), currentTimestamp);
                return ub.getTs().getTime();
            }
        });


        env.execute("flink learning project template");
    }
}
