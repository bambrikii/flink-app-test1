package org.bambrikii.examples.flink1;

import org.apache.flink.api.java.functions.KeySelector;

class Model1StringKeySelector implements KeySelector<Model1, String> {
    @Override
    public String getKey(Model1 value) {
        return value.getKey();
    }
}
