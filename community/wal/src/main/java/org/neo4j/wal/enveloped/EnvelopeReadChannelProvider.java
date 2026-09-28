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
package org.neo4j.wal.enveloped;

import java.io.IOException;
import org.neo4j.wal.WALService;

public interface EnvelopeReadChannelProvider {

    /** Opens a {@link EnvelopeReadChannel} of the lowest existing log file version
     *
     * @return {@link EnvelopeReadChannel} of the lowest existing log file version
     * @throws IllegalStateException if no log files are found
     */
    EnvelopeReadChannel openReadChannel() throws IOException;

    /**
     * Opens a {@link EnvelopeReadChannel} that points to the beginning of the file that may have the entry with the
     * provided index. If the index is higher than what exists in the log it will return the last file. If the index
     * is lower than what exists in the log then null is returned.
     *
     * @param fileWithIndex the index desired
     * @return {@link EnvelopeReadChannel} that points to the beginning of a file where prev index is lower than the
     * provided parameter.
     */
    EnvelopeReadChannel openReadChannel(long fileWithIndex) throws IOException;

    class DefaultEnvelopeReadChannelProvider implements EnvelopeReadChannelProvider {

        private final WALService walService;

        public DefaultEnvelopeReadChannelProvider(WALService walService) {
            this.walService = walService;
        }

        @Override
        public EnvelopeReadChannel openReadChannel() throws IOException {
            return walService.openReadChannel();
        }

        @Override
        public EnvelopeReadChannel openReadChannel(long fileWithIndex) throws IOException {
            return walService.openReadChannel(fileWithIndex);
        }
    }
}
