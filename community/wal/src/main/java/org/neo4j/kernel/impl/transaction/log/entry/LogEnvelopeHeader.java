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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.kernel.KernelVersion;

/**
 * A header that describes a subsection of a transaction that will fit within some logical segment of a log file.
 * <br>
 * For example, given the set of linear segments below, 3 transactions could be laid out as follows:
 * <br>
 * |__full__:__begin__||__end___:__begin__||___middle____||__end__------|
 * <br>
 * When there is not enough room for even a {@link LogEnvelopeHeader} to be written into a segment, that array of bytes
 * would be padded out with zeroes.
 */
public record LogEnvelopeHeader(
        // The type of the envelope
        EnvelopeType type,
        // The index of the entry that this envelope belongs to. An entry can be composed of a single FULL
        // or a combination of BEGIN, MIDDLE and END envelopes, in which case all of them will have the same
        // index.
        long index,
        // The length of the data payload within the envelope
        int payLoadLength,
        /*
         * The kernel version when the envelope was written. This value may be null in the case of MIDDLE and END
         * envelope types. If the internal structure of the envelope is written at the beginning of a segment/file,
         * then the version may be set, for example when a LogEntryCommit is written at such a boundary.
         */
        KernelVersion version,
        // The checksum of the previous envelope
        int previousChecksum,
        /*
         * The checksum of this envelope. Note that the checksum will include the values for the envelope type, the
         * payload length, the kernel version and the previous envelope's checksum.
         */
        int payloadChecksum) {

    public static final int HEADER_SIZE = Integer.BYTES // payload checksum
            + Byte.BYTES // envelope type
            + Integer.BYTES // payload length
            + Long.BYTES // entry index
            + Byte.BYTES // kernel version
            + Integer.BYTES; // previous checksum

    public static final int MAX_ZERO_PADDING_SIZE = Long.BYTES + LogEnvelopeHeader.HEADER_SIZE;

    public static final byte IGNORE_KERNEL_VERSION = -1;

    /**
     * Describes the type of envelope data written within the log file
     */
    public enum EnvelopeType {
        /**
         * An envelope of this type describes a contiguous length of zero-bytes at the end of a log file
         * <strong>PLEASE NOTE</strong> envelopes of this type MUST only ever appear once per segment and MUST be the
         * terminal envelope. In the case of the first segment of a file, a ZERO envelope will be the ONLY envelope
         * in the segment (other than the log header)
         */
        ZERO((byte) 0),
        /**
         * An envelope of this type describes a transaction that will fully fit within a segment block of a log file
         */
        FULL((byte) 1),
        /**
         * An envelope of this type describes the start of a transaction that would not fully fit within a segment
         * block of a log file
         */
        BEGIN((byte) 2),
        /**
         * An envelope of this type describes a subsection of a transaction that would not fully fit within a segment
         * block of a log file but would span the entire segment. Example:
         * <pre>
         * | <--- segment ---> | <--- segment ---> | <--- segment ---> | <--- segment ---> |
         * | <- file header -> | [###][##########] | [###############] | [####]            |
         * | "envelope type"     FULL  BEGIN          MIDDLE             END               |
         * </pre>
         */
        MIDDLE((byte) 3),
        /**
         * An envelope of this type describes the end of a transaction that would not fully fit within a segment
         * block of a log file
         */
        END((byte) 4);

        private static final EnvelopeType[] VALUES = EnvelopeType.values();
        public final byte typeValue;

        EnvelopeType(byte typeValue) {
            this.typeValue = typeValue;
        }

        public boolean isStarting() {
            return this == FULL || this == BEGIN;
        }

        public boolean isTerminating() {
            return this == FULL || this == END;
        }

        public static EnvelopeType of(byte type) {
            return VALUES[type];
        }
    }
}
