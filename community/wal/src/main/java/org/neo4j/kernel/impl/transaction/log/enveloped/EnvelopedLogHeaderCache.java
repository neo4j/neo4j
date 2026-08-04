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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

public interface EnvelopedLogHeaderCache {
    EnvelopedLogHeaderCache NULL = new EnvelopedLogHeaderCache() {
        @Override
        public void cache(LogHeader logHeader) {}

        @Override
        public Optional<LogHeader> getFirst() {
            return Optional.empty();
        }

        @Override
        public Optional<LogHeader> getLast() {
            return Optional.empty();
        }

        @Override
        public LongRange logVersionsRange() {
            return null;
        }

        @Override
        public LogHeader get(long version) {
            return null;
        }

        @Override
        public void forEachLogHeader(Consumer<LogHeader> logHeaderConsumer) {}

        @Override
        public void clear() {}

        @Override
        public void deleteTo(long version) {}

        @Override
        public void deleteFrom(long version) {}

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public List<LogHeader> currentLogHeaders(boolean reversed) {
            return List.of();
        }

        @Override
        public LogBinarySearch.BinarySearchReader binarySearchReader() {
            return null;
        }
    };

    void cache(LogHeader logHeader);

    Optional<LogHeader> getFirst();

    Optional<LogHeader> getLast();

    LongRange logVersionsRange();

    LogHeader get(long version);

    void forEachLogHeader(Consumer<LogHeader> logHeaderConsumer);

    void clear();

    void deleteTo(long version);

    void deleteFrom(long version);

    boolean isEmpty();

    List<LogHeader> currentLogHeaders(boolean reversed);

    LogBinarySearch.BinarySearchReader binarySearchReader();
}
