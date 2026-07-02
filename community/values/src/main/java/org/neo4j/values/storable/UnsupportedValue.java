/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.values.storable;

import static java.lang.String.format;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOf;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.Comparison;
import org.neo4j.values.ValueMapper;

/**
 * A value of a type that this instance does not understand.
 * <p>
 * These never originate locally and are not storable. They are produced when the bundled driver (used for Fabric and
 * server-side routing) receives a value from a downstream instance whose type is newer than what this instance knows
 * about. Rather than failing, the type is carried through opaquely and surfaced back to the client as an unsupported
 * type, so that a driver new enough to understand it still can. See {@link ValueWriter#writeUnsupported}.
 *
 */
public final class UnsupportedValue extends Value {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(UnsupportedValue.class);

    public static final String TYPE_NAME = "UNSUPPORTED";

    private final String name;
    private final String minProtocolVersion;
    private final Optional<String> message;

    UnsupportedValue(String name, String minProtocolVersion, String message) {
        this.name = Objects.requireNonNull(name, "name");
        this.minProtocolVersion = Objects.requireNonNull(minProtocolVersion, "minProtocolVersion");
        this.message = Optional.ofNullable(message);
    }

    /**
     * @return the remote type name, e.g. {@code "UUID"}.
     */
    public String name() {
        return name;
    }

    /**
     * @return the lowest Bolt protocol version that understands this type, e.g. {@code "6.1"}.
     */
    public String minProtocolVersion() {
        return minProtocolVersion;
    }

    /**
     * @return an optional human-readable description of why the type is unsupported.
     */
    public Optional<String> message() {
        return message;
    }

    @Override
    public boolean equals(Value other) {
        return other instanceof UnsupportedValue that
                && name.equals(that.name)
                && minProtocolVersion.equals(that.minProtocolVersion)
                && Objects.equals(message, that.message);
    }

    @Override
    public boolean isIncomparableType() {
        return true;
    }

    @Override
    protected int unsafeCompareTo(Value other) {
        UnsupportedValue that = (UnsupportedValue) other;
        int cmp = name.compareTo(that.name);
        if (cmp != 0) {
            return cmp;
        }
        cmp = minProtocolVersion.compareTo(that.minProtocolVersion);
        if (cmp != 0) {
            return cmp;
        }
        return Objects.compare(
                message.orElse(""), that.message.orElse(""), Comparator.nullsFirst(Comparator.naturalOrder()));
    }

    @Override
    public Comparison unsafeTernaryCompareTo(Value other) {
        // Unsupported values are not comparable under Comparability semantics, unless they are equal.
        return equals(other) ? Comparison.EQUAL : Comparison.UNDEFINED;
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writeUnsupported(name, minProtocolVersion, message.orElse(null));
    }

    @Override
    public Object asObjectCopy() {
        return name;
    }

    @Override
    public String prettyPrint() {
        return getTypeName();
    }

    @Override
    protected int computeHash() {
        return Objects.hash(name, minProtocolVersion, message);
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        return hashFunction.update(hash, hashCode());
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        throw new UnsupportedOperationException("Cannot map an unsupported type to a Java object: " + name);
    }

    @Override
    public String getTypeName() {
        return TYPE_NAME;
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.UNKNOWN;
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + sizeOf(name) + sizeOf(minProtocolVersion) + (message == null ? 0 : sizeOf(message));
    }

    @Override
    public String toString() {
        return format("%s(name=%s, minProtocolVersion=%s)", getClass().getSimpleName(), name, minProtocolVersion);
    }
}
