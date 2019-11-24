package org.bambrikii.examples.flink1;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.time.Instant;
import java.util.Random;

@Slf4j
public class SerializableThread extends Thread implements Serializable {
    private final String lock = "";
    private volatile Model1 model1;

    public SerializableThread() {
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            Model1 model1 = createModel1();
            takeMore(model1);
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                log.error(" > Error " + ex.getMessage(), ex);
            }
        }
        log.info(" > Exiting thread...");

    }

    public static Model1 createModel1() {
        Random random = new Random();
        int nextInt = random.nextInt(10);
        Model1 model1 = new Model1();
        model1.setKey("key-" + nextInt);
        model1.setValue(nextInt);
        model1.setTime(Instant.now().toEpochMilli());
        return model1;
    }

    public void takeMore(Model1 model1) {
        synchronized (lock) {
            if (this.model1 != null) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            this.model1 = model1;
            lock.notifyAll();
        }
    }

    public Model1 giveMeMore() {
        synchronized (lock) {
            if (this.model1 == null) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            Model1 model1 = this.model1;
            this.model1 = null;
            lock.notifyAll();
            return model1;
        }
    }
}
