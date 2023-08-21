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
package org.neo4j.kernel.api.impl.schema;

import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.values.storable.Value;

public class LuceneScoredEntityIndexProgressor implements IndexProgressor {
    private final ValuesIterator iterator;
    private final EntityValueClient client;
    private long limit;

    public LuceneScoredEntityIndexProgressor(
            ValuesIterator iterator, EntityValueClient client, IndexQueryConstraints constraints) {
        this.iterator = iterator;
        this.client = client;
        Iterators.skip(iterator, constraints.skip().orElse(0));
        this.limit = constraints.limit().orElse(Long.MAX_VALUE);
    }

    @Override
    public boolean next() {
        if (!iterator.hasNext() || limit == 0) {
            return false;
        }
        boolean accepted;
        do {
            long entityId = iterator.next();
            float score = iterator.currentScore();
            accepted = client.acceptEntity(entityId, score, (Value[]) null);
        } while (!accepted && iterator.hasNext());
        limit--;
        return accepted;
    }

    @Override
    public void close() {}
}
