/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index.impl.lucene;

import java.util.Collection;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.api.LegacyIndexHits;

public class CombinedIndexHits extends PrimitiveLongCollections.PrimitiveLongConcatingIterator implements LegacyIndexHits
{
    private final Collection<LegacyIndexHits> allIndexHits;
    private final int size;

    public CombinedIndexHits( Collection<LegacyIndexHits> iterators )
    {
        super( iterators.iterator() );
        this.allIndexHits = iterators;
        size = accumulatedSize( iterators );
    }

    private int accumulatedSize( Collection<LegacyIndexHits> iterators )
    {
        int result = 0;
        for ( LegacyIndexHits hits : iterators )
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
        for ( LegacyIndexHits hits : allIndexHits )
        {
            hits.close();
        }
    }

    @Override
    public float currentScore()
    {
        return ((LegacyIndexHits)currentIterator()).currentScore();
    }
}
