package org.example.antidote;

import org.apache.kafka.common.header.Headers;

import java.util.Arrays;

public record FailedDeserializationInfo(String topic, Headers headers, byte[] data, boolean isForKey,
                                        Exception exception) {
    @Override
    public String toString() {
        return "FailedDeserializationInfo{" +
                "topic='" + topic + '\'' +
                ", headers=" + headers +
                ", data=" + Arrays.toString(data) +
                ", isForKey=" + isForKey +
                ", exception=" + exception +
                '}';
    }
}
