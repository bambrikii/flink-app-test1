package org.bambrikii.examples.flink1;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;

@Slf4j
public class AsyncTestSource implements SourceFunction<Model1> {
    private SerializableThread thread;
    private volatile boolean isRunning;

    public AsyncTestSource() {
        thread = new SerializableThread();
    }

    public void start() {
        isRunning = true;
        thread.start();
    }

    public void stop() {
        isRunning = false;
        thread.interrupt();
    }

    @Override
    public void run(SourceContext<Model1> ctx) {
        while (isRunning) {
//            log.info(" > TestSource.Take...");
            Model1 model1 =
//                    thread.giveMeMore();
                    thread.createModel1();
//            log.info(" > TestSource: " + model1);
//            if (model1 != null) {
            ctx.collectWithTimestamp(model1, Instant.now().toEpochMilli());
//            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void cancel() {
        stop();
    }
}
