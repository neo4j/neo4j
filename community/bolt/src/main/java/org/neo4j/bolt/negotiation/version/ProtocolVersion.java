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
package org.neo4j.bolt.negotiation.version;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public record ProtocolVersion(short major, short minor, short range) implements Comparable<ProtocolVersion> {

    /**
     * Identifies the size of this structure when encoded (in bytes).
     */
    public static final int ENCODED_SIZE = 4;

    public static final int MAX_MAJOR_BIT = 0xFF;
    public static final int MAX_MINOR_BIT = 0xFF;

    /**
     * Provides an "invalid" protocol version which may be used for the purposes of padding empty fields or indicating that none of the proposed versions is
     * supported.
     */
    public static final ProtocolVersion INVALID = new ProtocolVersion((short) 0, (short) 0, (short) 0);

    /**
     * Provides a marker protocol version which is used to initiate a v2 protocol handshake.
     */
    public static final ProtocolVersion NEGOTIATION_V2 = new ProtocolVersion(MAX_MAJOR_BIT, 0x01);

    /**
     * Provides a maximum protocol version which may be used for the purposes of defining an
     * infinite upper range.
     */
    public static final ProtocolVersion MAX = new ProtocolVersion(MAX_MAJOR_BIT, MAX_MINOR_BIT, 0);

    public ProtocolVersion {
        if (major < 0 || major > MAX_MAJOR_BIT) {
            throw new IllegalArgumentException(
                    "Expected major version bit to be in bounds 0 <= x < " + MAX_MAJOR_BIT + ": Got " + major);
        }
        if (minor < 0 || minor > MAX_MINOR_BIT) {
            throw new IllegalArgumentException(
                    "Expected minor version bit to be in bounds 0 <= x < " + MAX_MAJOR_BIT + ": Got " + minor);
        }
        if (range > minor) {
            throw new IllegalArgumentException(
                    "Expected range bit to be in bounds 0 <= x < " + minor + ": Got " + range);
        }
    }

    public ProtocolVersion(int major, int minor, int range) {
        this((short) major, (short) minor, (short) range);
    }

    public ProtocolVersion(short major, short minor) {
        this(major, minor, (short) 0);
    }

    public ProtocolVersion(int major, int minor) {
        this((short) major, (short) minor);
    }

    public ProtocolVersion(int encoded) {
        this((short) (encoded & 0xFF), (short) (encoded >>> 8 & 0xFF), (short) (encoded >>> 16 & 0xFF));
    }

    /**
     * @deprecated Use {@link #major()} instead.
     */
    @Deprecated
    public short getMajor() {
        return this.major;
    }

    /**
     * @deprecated Use {@link #minor()} instead.
     */
    @Deprecated
    public short getMinor() {
        return this.minor;
    }

    /**
     * @deprecated Use {@link #range()} instead.
     */
    @Deprecated
    public short getRange() {
        return this.range;
    }

    /**
     * Evaluates whether this protocol version includes a range parameter.
     *
     * @return true if version represents a range, false otherwise.
     */
    public boolean hasRange() {
        return this.range != 0;
    }

    /**
     * Evaluates whether this protocol version refers to a handshake version rather than a protocol
     * version.
     * <p />
     * This flag is used to indicate support for a given modern revision of the handshake (or prior
     * version).
     *
     * @return true if this version refers to a handshake version.
     */
    public boolean isNegotiationVersion() {
        return this.major == MAX_MAJOR_BIT;
    }

    public boolean matches(ProtocolVersion other) {
        if (this.range == 0) {
            return this.equals(other);
        }

        if (this.major != other.major) {
            return false;
        }

        var lowerBound = this.minor - this.range;
        return other.minor >= lowerBound && other.minor <= this.minor;
    }

    public boolean isOlderThan(ProtocolVersion other) {
        return this.compareTo(other) < 0;
    }

    public boolean isNewerThan(ProtocolVersion other) {
        return this.compareTo(other) > 0;
    }

    public boolean isAtLeast(ProtocolVersion version) {
        return this.compareTo(version) >= 0;
    }

    public boolean isAtMost(ProtocolVersion version) {
        return this.compareTo(version) <= 0;
    }

    public int encode() {
        return (this.major & 0xFF) ^ ((this.minor & 0xFF) << 8) ^ ((this.range & 0xFF) << 16);
    }

    /**
     * Unwinds the versions represented by a protocol range into a list of canonical versions (e.g.
     * without their range parameter).
     * <p />
     * If the version represented by this instance does not include a range value, a list consisting
     * of only this object is returned instead.
     *
     * @return a list of canonical versions without their range parameter.
     */
    public List<ProtocolVersion> unwind() {
        if (this.range == 0) {
            return List.of(this);
        }

        return IntStream.range(this.minor - this.range, this.minor + 1)
                .mapToObj(minor -> new ProtocolVersion(this.major, minor))
                .toList();
    }

    @Override
    public int compareTo(ProtocolVersion o) {
        int result = Short.compare(this.major, o.major);
        return result != 0 ? result : Short.compare(this.minor, o.minor);
    }

    @Override
    public String toString() {
        if (this.range != 0) {
            var lowerBound = this.minor - this.range;

            return String.format("[%1$d.%2$d,%1$d.%3$d]", this.major, lowerBound, this.minor);
        }

        return String.format("%d.%d", this.major, this.minor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (ProtocolVersion) o;
        return this.major == that.major && this.minor == that.minor && this.range == that.range;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.major, this.minor, this.range);
    }
}
