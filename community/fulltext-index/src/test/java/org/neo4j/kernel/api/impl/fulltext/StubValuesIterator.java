/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;

class StubValuesIterator implements ValuesIterator
{
    private List<Long> entityIds = new ArrayList<>();
    private List<Float> scores = new ArrayList<>();
    private int nextIndex;

    public StubValuesIterator add( long entityId, float score )
    {
        entityIds.add( entityId );
        scores.add( score );
        return this;
    }

    @Override
    public int remaining()
    {
        return entityIds.size() - nextIndex;
    }

    @Override
    public float currentScore()
    {
        return scores.get( nextIndex - 1 );
    }

    @Override
    public long next()
    {
        long entityId = entityIds.get( nextIndex );
        nextIndex++;
        return entityId;
    }

    @Override
    public boolean hasNext()
    {
        return remaining() > 0;
    }

    @Override
    public long current()
    {
        return entityIds.get( nextIndex - 1 );
    }
}
