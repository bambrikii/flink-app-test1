package org.bambrikii.examples.flink1;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class Model1 implements Serializable {
    private String key;
    private int value;
    private long time;
}
