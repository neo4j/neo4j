/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.Iterator;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;

/**
 * Holds information vital for making {@link RelationshipGroupDefragmenter} work the way it does.
 *
 * The defragmenter goes potentially multiple rounds through the relationship group store and each round
 * selects groups from a range of node ids. This cache can cache the groups for the nodes in this range.
 *
 * First all group counts per node are updated ({@link #incrementGroupCount(long)}).
 * Then {@link #prepare(long)} is called from lowest node id (0) and given the maximum configured memory
 * given to this cache in its constructor the highest node id to cacne is returned. Then groups are
 * {@link #put(RelationshipGroupRecord)} and cached in here to later be {@link #iterator() retrieved}
 * where they are now ordered by node and type.
 * This will go on until the entire node range have been visited.
 *
 * @see RelationshipGroupDefragmenter
 */
public class RelationshipGroupCache implements Iterable<RelationshipGroupRecord>, AutoCloseable
{
    public static final int GROUP_ENTRY_SIZE = 1/*header*/ + 2/*type*/ + 6/*relationship id*/*3/*all directions*/;

    private final ByteArray groupCountCache;
    private final ByteArray cache;
    private final long highNodeId;
    private final LongArray offsets = AUTO.newDynamicLongArray( 100_000, 0 );
    private final byte[] scratch = new byte[GROUP_ENTRY_SIZE];
    private long fromNodeId;
    private long toNodeId;
    private long highCacheId;

    public RelationshipGroupCache( NumberArrayFactory arrayFactory, long maxMemory, long highNodeId )
    {
        this.groupCountCache = arrayFactory.newByteArray( highNodeId, new byte[2] );
        this.highNodeId = highNodeId;

        long memoryDedicatedToCounting = 2*highNodeId;
        long memoryLeftForGroupCache = maxMemory - memoryDedicatedToCounting;
        if ( memoryLeftForGroupCache < 0 )
        {
            throw new IllegalArgumentException( "Too little memory to cache any groups, provided " +
                    bytes( maxMemory ) + " where " + bytes( memoryDedicatedToCounting ) +
                    " was dedicated to group counting" );
        }
        this.cache = arrayFactory.newByteArray( memoryLeftForGroupCache / GROUP_ENTRY_SIZE, new byte[GROUP_ENTRY_SIZE] );
    }

    /**
     * Before caching any relationship groups all group counts for all nodes are incremented by calling
     * this method once for every encountered group (its node id).
     *
     * @param nodeId node to increment group count for.
     */
    public void incrementGroupCount( long nodeId )
    {
        int count = groupCountCache.getShort( nodeId, 0 ) & 0xFFFF;
        count++;
        if ( (count & ~0xFFFF) != 0 )
        {
            throw new IllegalStateException(
                    "Invalid number of relationship groups for node " + nodeId + " " + count );
        }
        groupCountCache.setShort( nodeId, 0, (short) count );
    }

    /**
     * Getter here because we can use this already allocated data structure for other things in and
     * around places where this group cache is used.
     */
    ByteArray getGroupCountCache()
    {
        return groupCountCache;
    }

    /**
     * Looks at max amount of configured memory (in constructor) and figures out for how many nodes their groups
     * can be cached. Before the first call to this method all {@link #incrementGroupCount(long)} calls
     * must have been made. After a call to this there should be a sequence of {@link #put(RelationshipGroupRecord)}
     * calls to cache the groups. If this call returns a node id which is lower than the highest node id in the
     * store then more rounds of caching should be performed after completing this round.
     *
     * @param fromNodeId inclusive
     * @return toNodeId exclusive
     */
    public long prepare( long fromNodeId )
    {
        cache.clear(); // this will have all the "first" bytes set to 0, which means !inUse
        this.fromNodeId = fromNodeId; // keep for use in put later on

        highCacheId = 0;
        for ( long nodeId = fromNodeId; nodeId < highNodeId; nodeId++ )
        {
            short count = groupCountCache.getShort( nodeId, 0 );
            if ( highCacheId + count > cache.length() )
            {
                // Cannot include this one, so up until the previous is good
                return this.toNodeId = nodeId;
            }
            offsets.set( rebase( nodeId ), highCacheId );
            highCacheId += count;
        }
        return this.toNodeId = highNodeId;
    }

    private long rebase( long toNodeId )
    {
        return toNodeId - fromNodeId;
    }

    /**
     * Caches a relationship group into this cache, it will be cached if the
     * {@link RelationshipGroupRecord#getOwningNode() owner} is within the {@link #prepare(long) prepared} range,
     * where {@code true} will be returned, otherwise {@code false}.
     *
     * @param groupRecord {@link RelationshipGroupRecord} to cache.
     * @return whether or not the group was cached, i.e. whether or not it was within the prepared range.
     */
    public boolean put( RelationshipGroupRecord groupRecord )
    {
        long nodeId = groupRecord.getOwningNode();
        assert nodeId < highNodeId;
        if ( nodeId < fromNodeId || nodeId >= toNodeId )
        {
            return false;
        }

        long baseIndex = offsets.get( rebase( nodeId ) );
        // grouCount is extra validation, really
        int groupCount = groupCountCache.getShort( nodeId, 0 );
        long index = scanForFreeFrom( baseIndex, groupCount, groupRecord.getType() );

        // Put the group at this index
        cache.setByte( index, 0, (byte) 1 );
        cache.setShort( index, 1, (short) groupRecord.getType() );
        cache.set6ByteLong( index, 1 + 2, groupRecord.getFirstOut() );
        cache.set6ByteLong( index, 1 + 2 + 6, groupRecord.getFirstIn() );
        cache.set6ByteLong( index, 1 + 2 + 6 + 6, groupRecord.getFirstLoop() );
        return true;
    }

    private long scanForFreeFrom( long startIndex, int groupCount, int type )
    {
        long desiredIndex = -1;
        long freeIndex = -1;
        for ( int i = 0; i < groupCount; i++ )
        {
            long candidateIndex = startIndex + i;
            boolean free = cache.getByte( candidateIndex, 0 ) == 0;
            if ( free )
            {
                freeIndex = candidateIndex;
                break;
            }

            if ( desiredIndex == -1 )
            {
                int existingType = cache.getShort( candidateIndex, 1 ) & 0xFFFF;
                if ( existingType == type )
                {
                    throw new IllegalStateException( "Tried to put multiple groups with same type " + type );
                }

                if ( type < existingType )
                {
                    // This means that the groups have arrived here out of order, please put this group
                    // in the correct place, not at the end
                    desiredIndex = candidateIndex;
                }
            }
        }

        if ( freeIndex == -1 )
        {
            throw new IllegalStateException( "There's no room for me for startIndex:" + startIndex +
                    " with a group count of " + groupCount + ". This means that there's an asymmetry between calls " +
                    "to incrementGroupCount and actual contents sent into put" );
        }

        // For the future: Instead of doing the sorting here right away be doing the relatively expensive move
        // of potentially multiple items one step to the right in the array, then an idea is to simply mark
        // this group as in need of sorting and then there may be a step later which can use all CPUs
        // on the machine, jumping from group to group and see if the "needs sorting" flag has been raised
        // and if so sort that group. This is fine as it is right now because the groups put into this cache
        // will be almost entirely sorted, since we come here straight after import. Although if this thing
        // is to be used as a generic relationship group defragmenter this sorting will have to be fixed
        // to something like what is described above in this comment.
        if ( desiredIndex != -1 )
        {
            moveRight( desiredIndex, freeIndex );
            return desiredIndex;
        }
        return freeIndex;
    }

    private void moveRight( long fromIndex, long toIndex )
    {
        for ( long index = toIndex; index > fromIndex; index-- )
        {
            cache.get( index-1, scratch );
            cache.set( index, scratch );
        }
    }

    /**
     * @return cached {@link RelationshipGroupRecord} sorted by node id and then type id.
     */
    @Override
    public Iterator<RelationshipGroupRecord> iterator()
    {
        return new PrefetchingIterator<RelationshipGroupRecord>()
        {
            private long cursor;
            private long nodeId = fromNodeId;
            private int countLeftForThisNode = groupCountCache.getShort( nodeId, 0 );
            {
                findNextNodeWithGroupsIfNeeded();
            }

            @Override
            protected RelationshipGroupRecord fetchNextOrNull()
            {
                while ( cursor < highCacheId )
                {
                    RelationshipGroupRecord group = null;
                    if ( cache.getByte( cursor, 0 ) == 1 )
                    {
                        // Here we have an alive group
                        group = new RelationshipGroupRecord( -1 ).initialize( true,
                                cache.getShort( cursor, 1 ) & 0xFFFF,
                                cache.get6ByteLong( cursor, 1 + 2 ),
                                cache.get6ByteLong( cursor, 1 + 2 + 6 ),
                                cache.get6ByteLong( cursor, 1 + 2 + 6 + 6 ),
                                nodeId,
                                // Special: we want to convey information about how many groups are coming
                                // after this one so that chains can be ordered accordingly in the store
                                // so this isn't at all "next" in the true sense of chain next.
                                countLeftForThisNode-1 );
                    }

                    cursor++;
                    countLeftForThisNode--;
                    findNextNodeWithGroupsIfNeeded();

                    if ( group != null )
                    {
                        return group;
                    }
                }
                return null;
            }

            private void findNextNodeWithGroupsIfNeeded()
            {
                if ( countLeftForThisNode == 0 )
                {
                    do
                    {
                        nodeId++;
                        countLeftForThisNode = nodeId >= groupCountCache.length() ? 0 :
                            groupCountCache.getShort( nodeId, 0 );
                    }
                    while ( countLeftForThisNode == 0 && nodeId < groupCountCache.length() );
                }
            }
        };
    }

    @Override
    public void close()
    {
        cache.close();
        offsets.close();
    }
}
