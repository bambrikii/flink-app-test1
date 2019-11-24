package org.bambrikii.examples.flink1;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.time.Instant;

@Slf4j
public class App1 {
    public static void main(String[] args) throws Exception {
        ParameterTool param = ParameterTool.fromPropertiesFile(App1.class.getResourceAsStream("App1-flink.properties"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//        env.getConfig().
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setMaxParallelism(1);
        env.setParallelism(1);

        log.info("start");

        AsyncTestSource asyncTestSource = new AsyncTestSource();
        asyncTestSource.start();

        DataStreamSource<Model1> streamSource = env
                .addSource(asyncTestSource);

        KeyedStream<Model1, String> keyedStream = streamSource
                .keyBy(new Model1StringKeySelector());

        SingleOutputStreamOperator<Object> stream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.seconds(0)))
                .trigger(new Trigger<Model1, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Model1 element, long timestamp, TimeWindow window, TriggerContext ctx) {
                        log.info(" > Element:             " + element + ", " + ctx.getCurrentProcessingTime() + " " + ctx.getCurrentWatermark() + " " + window);
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
                        log.info("processing time");
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
                        log.info("event time");
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) {
                        log.info("clear");
                    }
                })
                .process(new ProcessWindowFunction<Model1, Object, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Model1> elements, Collector<Object> out) {
                        log.info(" > Processing...");
                        for (Model1 element : elements) {
                            log.info(" > Process: " + s + " -     " + element);
                            out.collect(element);
                        }
                    }
                });

        streamSource
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Model1>() {
                    @Override
                    public long extractTimestamp(Model1 element, long previousElementTimestamp) {
                        return element.getTime();
                    }

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark((Instant.now().toEpochMilli() / 1000) * 1000 - 1000);
                    }
                })
                .addSink(new SinkFunction<Model1>() {
                    @Override
                    public void invoke(Model1 value, Context context) {
                        log.info(" > Process Watermarked: " + value + ", " + context.currentWatermark() + " " + context.timestamp() + " " + context.currentProcessingTime());
                    }
                })
        ;

        stream.addSink(new SinkFunction<Object>() {
            @Override
            public void invoke(Object value, Context context) {
                log.info(" > Sink:               " + value + ", ");
            }
        });

        env.execute();

        log.info("stop");
    }
}
