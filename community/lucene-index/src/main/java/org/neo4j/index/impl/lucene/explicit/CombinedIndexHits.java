/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.index.impl.lucene.explicit;

import java.util.Collection;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.api.ExplicitIndexHits;

public class CombinedIndexHits extends PrimitiveLongCollections.PrimitiveLongConcatingIterator implements
        ExplicitIndexHits
{
    private final Collection<ExplicitIndexHits> allIndexHits;
    private final int size;

    public CombinedIndexHits( Collection<ExplicitIndexHits> iterators )
    {
        super( iterators.iterator() );
        this.allIndexHits = iterators;
        size = accumulatedSize( iterators );
    }

    private int accumulatedSize( Collection<ExplicitIndexHits> iterators )
    {
        int result = 0;
        for ( ExplicitIndexHits hits : iterators )
        {
            result += hits.size();
        }
        return result;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void close()
    {
        for ( ExplicitIndexHits hits : allIndexHits )
        {
            hits.close();
        }
    }

    @Override
    public float currentScore()
    {
        return ((ExplicitIndexHits)currentIterator()).currentScore();
    }
}
