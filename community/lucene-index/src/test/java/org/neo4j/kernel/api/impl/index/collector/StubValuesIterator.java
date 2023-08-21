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
package org.neo4j.kernel.api.impl.index.collector;

import org.eclipse.collections.api.factory.primitive.FloatLists;
import org.eclipse.collections.api.list.primitive.MutableFloatList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

public class StubValuesIterator implements ValuesIterator {
    private final MutableLongList entityIds = LongLists.mutable.empty();
    private final MutableFloatList scores = FloatLists.mutable.empty();
    private int nextIndex;

    public StubValuesIterator add(long entityId, float score) {
        entityIds.add(entityId);
        scores.add(score);
        return this;
    }

    @Override
    public int remaining() {
        return entityIds.size() - nextIndex;
    }

    @Override
    public float currentScore() {
        return scores.get(nextIndex - 1);
    }

    @Override
    public long next() {
        return entityIds.get(nextIndex++);
    }

    @Override
    public boolean hasNext() {
        return remaining() > 0;
    }

    @Override
    public long current() {
        return entityIds.get(nextIndex - 1);
    }
}
