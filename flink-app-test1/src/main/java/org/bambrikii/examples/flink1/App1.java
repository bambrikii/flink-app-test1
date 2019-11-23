package org.bambrikii.examples.flink1;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;

public class App1 {
    public static void main(String[] args) throws Exception {
        LocalEnvironment env = ExecutionEnvironment.createLocalEnvironment();
//        env.getConfig().


        env.execute();
    }
}
