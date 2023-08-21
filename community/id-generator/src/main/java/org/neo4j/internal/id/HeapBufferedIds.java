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
package org.neo4j.internal.id;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.neo4j.io.IOUtils;

/**
 * Keeps buffered IDs in the heap in an unbounded queue.
 */
class HeapBufferedIds implements BufferedIds {
    private final ConcurrentLinkedDeque<Entry> queue = new ConcurrentLinkedDeque<>();

    @Override
    public void write(IdController.TransactionSnapshot snapshot, List<BufferingIdGeneratorFactory.IdBuffer> idBuffers)
            throws IOException {
        queue.add(new Entry(snapshot, idBuffers));
    }

    @Override
    public void read(BufferedIds.BufferedIdVisitor visitor) throws IOException {
        Entry entry;
        while ((entry = queue.peek()) != null) {
            if (!visitor.startChunk(entry.snapshot)) {
                // Snapshot still open
                break;
            }

            try {
                queue.remove();
                for (var idBuffer : entry.idBuffers) {
                    visitor.startType(idBuffer.idTypeOrdinal());
                    try {
                        var ids = idBuffer.ids().iterator();
                        while (ids.hasNext()) {
                            visitor.id(ids.next());
                        }
                    } finally {
                        visitor.endType();
                    }
                }
            } finally {
                visitor.endChunk();
                IOUtils.closeAll(entry.idBuffers);
            }
        }
    }

    @Override
    public void close() throws IOException {}

    private record Entry(
            IdController.TransactionSnapshot snapshot, List<BufferingIdGeneratorFactory.IdBuffer> idBuffers) {}
}
