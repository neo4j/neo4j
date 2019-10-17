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
package org.neo4j.index.internal.gbptree;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

class SizeEstimationMonitor implements SeekCursor.Monitor
{
    private static final int DEPTH_NOT_DECIDED = -1;

    private final MutableIntObjectMap<Stats> depthStats = IntObjectMaps.mutable.empty();
    private int treeDepth = DEPTH_NOT_DECIDED; // un-initialized
    private boolean allHaveSameDepth = true;

    @Override
    public void internalNode( int depth, int keyCount )
    {
        depthStats.getIfAbsentPut( depth, Stats::new ).add( keyCount + 1 );
    }

    @Override
    public void leafNode( int depth, int keyCount )
    {
        depthStats.getIfAbsentPut( depth, Stats::new ).add( keyCount );
        if ( treeDepth == DEPTH_NOT_DECIDED )
        {
            treeDepth = depth;
        }
        else if ( treeDepth != depth )
        {
            allHaveSameDepth = false;
        }
    }

    void clear()
    {
        treeDepth = DEPTH_NOT_DECIDED;
        allHaveSameDepth = true;
        depthStats.clear();
    }

    boolean isConsistent()
    {
        return allHaveSameDepth;
    }

    long estimateNumberOfKeys()
    {
        double count = 1;
        for ( int i = 0; i <= treeDepth; i++ )
        {
            count *= depthStats.get( i ).averageNumberOfKeys();
        }
        return (long) count;
    }

    private static class Stats
    {
        int numberOfVisitedNodes;
        int numberOfKeys;

        void add( int keyCount )
        {
            numberOfVisitedNodes++;
            numberOfKeys += keyCount;
        }

        double averageNumberOfKeys()
        {
            return (double) numberOfKeys / numberOfVisitedNodes;
        }
    }
}
