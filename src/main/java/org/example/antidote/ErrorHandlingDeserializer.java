package org.example.antidote;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.function.Function;

public class ErrorHandlingDeserializer<T> implements Deserializer<T> {
    /**
     * Supplier for a T when deserialization fails.
     */
    public static final String KEY_FUNCTION = "example.deserializer.key.function";

    /**
     * Supplier for a T when deserialization fails.
     */
    public static final String VALUE_FUNCTION = "example.deserializer.value.function";

    /**
     * Property name for the delegate key deserializer.
     */
    public static final String KEY_DESERIALIZER_CLASS = "example.deserializer.key.delegate.class";

    /**
     * Property name for the delegate value deserializer.
     */
    public static final String VALUE_DESERIALIZER_CLASS = "example.deserializer.value.delegate.class";

    private Deserializer<T> delegate;

    private boolean isForKey;

    private Function<FailedDeserializationInfo, T> failedDeserializationFunction;

    public ErrorHandlingDeserializer() {

    }

    public ErrorHandlingDeserializer(Deserializer<T> delegate) {
        this.delegate = this.setupDelegate(delegate);
    }

    public boolean isForKey() {
        return this.isForKey;
    }

    public void setForKey(boolean isForKey) {
        this.isForKey = isForKey;
    }

    public void setFailedDeserializationFunction(Function<FailedDeserializationInfo, T> failedDeserializationFunction) {
        this.failedDeserializationFunction = failedDeserializationFunction;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (this.delegate == null) {
            setupDelegate(configs, isKey ? KEY_DESERIALIZER_CLASS : VALUE_DESERIALIZER_CLASS);
        }
        if (this.delegate == null) {
            throw new IllegalStateException("No delegate deserializer configured");
        }
        this.delegate.configure(configs, isKey);
        this.isForKey = isKey;
        if (this.failedDeserializationFunction == null) {
            setupFunction(configs, isKey ? KEY_FUNCTION : VALUE_FUNCTION);
        }
    }

    public void setupDelegate(Map<String, ?> configs, String configKey) {
        if (configs.containsKey(configKey)) {
            try {
                Object value = configs.get(configKey);
                Class<?> clazz = value instanceof Class<?> ? (Class<?>) value : Class.forName((String) value);
                Object delegate = clazz.getDeclaredConstructor().newInstance();
                this.delegate = this.setupDelegate(delegate);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Deserializer<T> setupDelegate(Object delegate) {
        if (!(delegate instanceof Deserializer)) {
            throw new IllegalArgumentException("'delegate' must be a 'Deserializer', not a " + delegate.getClass().getName());
        }
        return (Deserializer<T>) delegate;
    }

    @SuppressWarnings("unchecked")
    private void setupFunction(Map<String, ?> configs, String configKey) {
        if (configs.containsKey(configKey)) {
            try {
                Object value = configs.get(configKey);
                Class<?> clazz = value instanceof Class<?> ? (Class<?>) value : Class.forName((String) value);
                if (!Function.class.isAssignableFrom(clazz)) {
                    throw new IllegalArgumentException("'function' must be a 'Function', not a " + clazz.getName());
                }
                this.failedDeserializationFunction = (Function<FailedDeserializationInfo, T>)
                        clazz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return this.delegate.deserialize(topic, data);
        } catch (Exception e) {
            return recoverFromSupplier(topic, null, data, e);
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        try {
            return this.delegate.deserialize(topic, headers, data);
        } catch (Exception e) {
            return recoverFromSupplier(topic, headers, data, e);
        }
    }

    private T recoverFromSupplier(String topic, Headers headers, byte[] data, Exception exception) {
        if (this.failedDeserializationFunction != null) {
            FailedDeserializationInfo failedDeserializationInfo =
                    new FailedDeserializationInfo(topic, headers, data, this.isForKey, exception);
            return this.failedDeserializationFunction.apply(failedDeserializationInfo);
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        if (this.delegate != null) {
            this.delegate.close();
        }
    }
}
