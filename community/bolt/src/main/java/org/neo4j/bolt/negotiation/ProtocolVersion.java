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
package org.neo4j.bolt.negotiation;

import java.util.Objects;

public record ProtocolVersion(short major, short minor, short range) implements Comparable<ProtocolVersion> {

    /**
     * Identifies the size of this structure when encoded (in bytes).
     */
    public static final int ENCODED_SIZE = 4;

    /**
     * Provides an "invalid" protocol version which may be used for the purposes of padding empty fields or indicating that none of the proposed versions is
     * supported.
     */
    public static final ProtocolVersion INVALID = new ProtocolVersion((short) 0, (short) 0, (short) 0);

    public static final int MAX_MAJOR_BIT = 255;
    public static final int MAX_MINOR_BIT = 255;

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

    public short getMajor() {
        return this.major;
    }

    public short getMinor() {
        return this.minor;
    }

    public short getRange() {
        return this.range;
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

    public int encode() {
        return (this.major & 0xFF) ^ ((this.minor & 0xFF) << 8) ^ ((this.range & 0xFF) << 16);
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
