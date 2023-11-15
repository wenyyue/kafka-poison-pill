package org.example.antidote;

import java.util.function.Function;

public class FailedDeserializationFunction<T extends FailedDeserializationInfo, R> implements Function<T, R> {
    public FailedDeserializationFunction() {

    }

    @Override
    public R apply(FailedDeserializationInfo failedDeserializationInfo) {
        // Here we can put the inappropriate message into dead letter.
        return null;
    }
}
