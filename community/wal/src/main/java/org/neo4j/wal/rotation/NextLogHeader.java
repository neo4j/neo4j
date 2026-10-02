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
package org.neo4j.wal.rotation;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.wal.entry.LogFormat;

/**
 * The values written into the header of the file a rotation creates: the kernel version and log format of the new
 * file, and the tail (last append index, checksum and term) of the file being closed.
 * <p>
 * Instances are only produced inside a rotation critical section, by {@link Builder#build(NextLogHeader)}.
 *
 * @param kernelVersion    kernel version of the new file, or {@code null} where the file decides it by other means
 *                         (merged log files take it from their {@code LogHeaderFactory}).
 * @param logFormat        log format of the new file, or {@code null} under the same conditions as kernelVersion.
 * @param lastAppendIndex  append index of the last entry in the file being closed.
 * @param previousChecksum checksum of the last entry in the file being closed.
 * @param lastTerm         term of the last entry in the file being closed.
 */
public record NextLogHeader(
        KernelVersion kernelVersion, LogFormat logFormat, long lastAppendIndex, int previousChecksum, long lastTerm) {

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collects the header values a caller knows explicitly. Anything left unset is taken from the rotating file's
     * live state, which concurrent appenders mean is only stable inside the rotation critical section; so only the
     * rotating file calls {@link #build(NextLogHeader)}, after the outgoing file has been flushed.
     * <p>
     * Explicit values must come from a caller that already has exclusive access to the log, such as an appender
     * holding the log file lock or the envelope channel's own rotation callback.
     */
    public static final class Builder {
        private KernelVersion kernelVersion;
        private LogFormat logFormat;
        private boolean hasTail;
        private long lastAppendIndex;
        private int previousChecksum;
        private boolean hasTerm;
        private long lastTerm;

        private Builder() {}

        public Builder withKernelVersion(KernelVersion kernelVersion) {
            this.kernelVersion = kernelVersion;
            return this;
        }

        public Builder withLogFormat(LogFormat logFormat) {
            this.logFormat = logFormat;
            return this;
        }

        public Builder withTail(long lastAppendIndex, int previousChecksum) {
            this.hasTail = true;
            this.lastAppendIndex = lastAppendIndex;
            this.previousChecksum = previousChecksum;
            return this;
        }

        public Builder withTail(long lastAppendIndex, int previousChecksum, long lastTerm) {
            withTail(lastAppendIndex, previousChecksum);
            this.hasTerm = true;
            this.lastTerm = lastTerm;
            return this;
        }

        /**
         * @param live the rotating file's live state, read inside the rotation critical section.
         * @return the explicit values set on this builder, with any unset ones taken from {@code live}.
         */
        public NextLogHeader build(NextLogHeader live) {
            return new NextLogHeader(
                    kernelVersion != null ? kernelVersion : live.kernelVersion(),
                    logFormat != null ? logFormat : live.logFormat(),
                    hasTail ? lastAppendIndex : live.lastAppendIndex(),
                    hasTail ? previousChecksum : live.previousChecksum(),
                    hasTerm ? lastTerm : live.lastTerm());
        }
    }
}
