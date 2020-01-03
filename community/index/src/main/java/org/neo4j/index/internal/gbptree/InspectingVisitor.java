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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

public class InspectingVisitor<KEY, VALUE> extends GBPTreeVisitor.Adaptor<KEY,VALUE>
{
    private final MutableLongList internalNodes = LongLists.mutable.empty();
    private final MutableLongList leafNodes = LongLists.mutable.empty();
    private final MutableLongList allNodes = LongLists.mutable.empty();
    private final Map<Long,Integer> allKeyCounts = new HashMap<>();
    private final List<LongList> nodesPerLevel = new ArrayList<>();
    private final List<FreelistEntry> allFreelistEntries = new ArrayList<>();
    private long rootNode;
    private int lastLevel;
    private TreeState treeState;
    private MutableLongList currentLevelNodes;
    private long currentFreelistPage;

    public InspectingVisitor()
    {
        clear();
    }

    public GBPTreeInspection<KEY,VALUE> get()
    {
        final List<ImmutableLongList> immutableNodesPerLevel = nodesPerLevel.stream()
                .map( LongLists.immutable::ofAll )
                .collect( Collectors.toList() );
        return new GBPTreeInspection<>(
                LongLists.immutable.ofAll( internalNodes ),
                LongLists.immutable.ofAll( leafNodes ),
                LongLists.immutable.ofAll( allNodes ),
                unmodifiableMap( allKeyCounts ),
                immutableNodesPerLevel,
                unmodifiableList( allFreelistEntries ),
                rootNode,
                lastLevel,
                treeState );
    }

    @Override
    public void treeState( Pair<TreeState,TreeState> statePair )
    {
        this.treeState = TreeStatePair.selectNewestValidState( statePair );
    }

    @Override
    public void beginLevel( int level )
    {
        lastLevel = level;
        currentLevelNodes = LongLists.mutable.empty();
        nodesPerLevel.add( currentLevelNodes );
    }

    @Override
    public void beginNode( long pageId, boolean isLeaf, long generation, int keyCount )
    {
        if ( lastLevel == 0 )
        {
            if ( rootNode != -1 )
            {
                throw new IllegalStateException( "Expected to only have a single node on level 0" );
            }
            rootNode = pageId;
        }

        currentLevelNodes.add( pageId );
        allNodes.add( pageId );
        allKeyCounts.put( pageId, keyCount );
        if ( isLeaf )
        {
            leafNodes.add( pageId );
        }
        else
        {
            internalNodes.add( pageId );
        }
    }

    @Override
    public void beginFreelistPage( long pageId )
    {
        currentFreelistPage = pageId;
    }

    @Override
    public void freelistEntry( long pageId, long generation, int pos )
    {
        allFreelistEntries.add( new FreelistEntry( currentFreelistPage, pos, pageId, generation ) );
    }

    private void clear()
    {
        rootNode = -1;
        lastLevel = -1;
    }

    static class FreelistEntry
    {
        final long freelistPageId;
        final int pos;
        final long id;
        final long generation;

        private FreelistEntry( long freelistPageId, int pos, long id, long generation )
        {
            this.freelistPageId = freelistPageId;
            this.pos = pos;
            this.id = id;
            this.generation = generation;
        }
    }
}
