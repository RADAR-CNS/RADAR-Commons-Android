package org.apache.avro.util;

public class IdentityReference<T> {
    private final T value;

    public IdentityReference(T value) {
        this.value = value;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdentityReference<?> that = (IdentityReference<?>) o;
        return value == that.value;
    }

    public int hashCode() {
        return System.identityHashCode(value);
    }
}
