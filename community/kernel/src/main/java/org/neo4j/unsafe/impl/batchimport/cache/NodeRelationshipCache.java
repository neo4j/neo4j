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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.Direction;

/**
 * Caches of parts of node store and relationship group store. A crucial part of batch import where
 * any random access must be covered by this cache. All I/O, both read and write must be sequential.
 */
public class NodeRelationshipCache implements MemoryStatsVisitor.Visitable
{
    private static final long EMPTY = -1;
    private static final byte[] DEFAULT_VALUE = new byte[10];
    private static final long MAX_RELATIONSHIP_ID = (1L << 48/*6B*/) - 2/*reserving -1 as legal default value*/;
    private static final int ID_SIZE = 6;
    private static final int COUNT_SIZE = 4;
    private static final int ID_AND_COUNT_SIZE = ID_SIZE + COUNT_SIZE;
    private static final int SPARSE_ID_OFFSET = 0;
    private static final int SPARSE_COUNT_OFFSET = ID_SIZE;
    static
    {
        // This looks odd, but we're using the array itself to create a default byte[] for another
        ByteArray array = NumberArrayFactory.HEAP.newByteArray( 1, DEFAULT_VALUE.clone() );
        array.set6ByteLong( 0, SPARSE_ID_OFFSET, EMPTY );
        array.setInt( 0, SPARSE_COUNT_OFFSET, 0 );
        array.get( 0, DEFAULT_VALUE );
    }

    private final ByteArray array;
    private final int denseNodeThreshold;
    private final RelGroupCache relGroupCache;

    public NodeRelationshipCache( NumberArrayFactory arrayFactory, int denseNodeThreshold )
    {
        this( arrayFactory, denseNodeThreshold, 1_000_000, 0 );
    }

    NodeRelationshipCache( NumberArrayFactory arrayFactory, int denseNodeThreshold, int chunkSize, long base )
    {
        this.array = arrayFactory.newDynamicByteArray( chunkSize, DEFAULT_VALUE );
        this.denseNodeThreshold = denseNodeThreshold;
        this.relGroupCache = new RelGroupCache( arrayFactory, chunkSize, base );
    }

    /**
     * Increment relationship count for {@code nodeId}.
     * @param nodeId node to increment relationship count for.
     * @return count after the increment.
     */
    public int incrementCount( long nodeId )
    {
        ByteArray array = this.array.at( nodeId );
        int count = getCount( array, nodeId ) + 1;
        setCount( array, nodeId, count );
        return count;
    }

    private void setCount( ByteArray array, long nodeId, int count )
    {
        array.setInt( nodeId, SPARSE_COUNT_OFFSET, count );
    }

    private static int getCount( ByteArray array, long nodeId )
    {
        return array.getInt( nodeId, SPARSE_COUNT_OFFSET );
    }

    public boolean isDense( long nodeId )
    {
        return isDense( array, nodeId );
    }

    private boolean isDense( ByteArray array, long nodeId )
    {
        if ( denseNodeThreshold == EMPTY )
        {   // We haven't initialized the rel group cache yet
            return false;
        }

        return getCount( array, nodeId ) >= denseNodeThreshold;
    }

    public long getAndPutRelationship( long nodeId, int type, Direction direction, long firstRelId,
            boolean incrementCount )
    {
        if ( firstRelId > MAX_RELATIONSHIP_ID )
        {
            throw new IllegalArgumentException( "Illegal relationship id, max is " + MAX_RELATIONSHIP_ID );
        }

        /*
         * OK so the story about counting goes: there's an initial pass for counting number of relationships
         * per node, globally, not per type/direction. After that the relationship group cache is initialized
         * and the relationship stage is executed where next pointers are constructed. That forward pass should
         * not increment the global count, but it should increment the type/direction counts.
         */

        ByteArray array = this.array.at( nodeId );
        long existingId = all48Bits( array, nodeId, SPARSE_ID_OFFSET );
        if ( isDense( array, nodeId ) )
        {
            if ( existingId == EMPTY )
            {
                existingId = relGroupCache.allocate( type, direction, firstRelId, incrementCount );
                setRelationshipId( array, nodeId, existingId );
                return EMPTY;
            }
            return relGroupCache.putRelationship( existingId, type, direction, firstRelId, incrementCount );
        }

        // Don't increment count for sparse node since that has already been done in a previous pass
        setRelationshipId( array, nodeId, firstRelId );
        return existingId;
    }

    private void setRelationshipId( ByteArray array, long nodeId, long firstRelId )
    {
        array.set6ByteLong( nodeId, SPARSE_ID_OFFSET, firstRelId );
    }

    private long getRelationshipId( ByteArray array, long nodeId )
    {
        return array.get6ByteLong( nodeId, SPARSE_ID_OFFSET );
    }

    private static long all48Bits( ByteArray array, long index, int offset )
    {
        return all48Bits( array.get6ByteLong( index, offset ) );
    }

    private static long all48Bits( long raw )
    {
        return raw == -1L ? raw : raw & 0xFFFFFFFFFFFFL;
    }

    /**
     * Used when setting node nextRel fields. Gets the first relationship for this node,
     * or the first relationship group id (where it it first visits all the groups before returning the first one).
     */
    public long getFirstRel( long nodeId, GroupVisitor visitor )
    {
        ByteArray array = this.array.at( nodeId );
        long id = getRelationshipId( array, nodeId );
        if ( isDense( array, nodeId ) )
        {   // Indirection into rel group cache
            return relGroupCache.visitGroups( nodeId, id, visitor );
        }

        return id;
    }

    public void clearRelationships()
    {
        long length = array.length();
        for ( long nodeId = 0; nodeId < length; nodeId++ )
        {
            if ( !isDense( nodeId ) )
            {
                setRelationshipId( array, nodeId, -1 );
            }
        }
        relGroupCache.clearRelationships();
    }

    public int getCount( long nodeId, int type, Direction direction )
    {
        ByteArray array = this.array.at( nodeId );
        if ( isDense( array, nodeId ) )
        {   // Indirection into rel group cache
            long id = getRelationshipId( array, nodeId );
            return id == EMPTY ? 0 : relGroupCache.getCount( id, type, direction );
        }

        return getCount( array, nodeId );
    }

    public interface GroupVisitor
    {
        /**
         * Visits with data required to create a relationship group.
         *
         * @param nodeId node id.
         * @param type relationship type.
         * @param next next relationship group.
         * @param out first outgoing relationship id.
         * @param in first incoming relationship id.
         * @param loop first loop relationship id.
         * @return the created relationship group id.
         */
        long visit( long nodeId, int type, long next, long out, long in, long loop );
    }

    public static final GroupVisitor NO_GROUP_VISITOR = new GroupVisitor()
    {
        @Override
        public long visit( long nodeId, int type, long next, long out, long in, long loop )
        {
            return -1;
        }
    };

    private static class RelGroupCache implements AutoCloseable, MemoryStatsVisitor.Visitable
    {
        private static final int TYPE_SIZE = 2;
        private static final int NEXT_OFFSET = 0;
        private static final int TYPE_OFFSET = 6;
        private static final int BASE_IDS_OFFSET = ID_SIZE + TYPE_SIZE;
        private static final byte[] DEFAULT_VALUE =
                new byte[ID_SIZE/*next*/ + TYPE_SIZE + (ID_SIZE + COUNT_SIZE) * Direction.values().length];
        static
        {
            ByteArray defaultArray = NumberArrayFactory.HEAP.newByteArray( 1, DEFAULT_VALUE.clone() );
            defaultArray.set6ByteLong( 0, NEXT_OFFSET, EMPTY );
            defaultArray.setShort( 0, TYPE_OFFSET, (short) EMPTY );
            for ( int i = 0, offsetBase = BASE_IDS_OFFSET; i < Direction.values().length;
                    i++, offsetBase += ID_AND_COUNT_SIZE )
            {
                defaultArray.set6ByteLong( 0, offsetBase, EMPTY );
                defaultArray.setInt( 0, offsetBase + ID_SIZE, 0 );
            }
            defaultArray.get( 0, DEFAULT_VALUE );
        }

        // Used for testing high id values. Should always be zero in production
        private final long base;
        private final ByteArray array;
        private final AtomicLong nextFreeId;

        RelGroupCache( NumberArrayFactory arrayFactory, long chunkSize, long base )
        {
            this.base = base;
            assert chunkSize > 0;
            // We can use this array to have "entries" accommodating one entire group, e.g:
            // - next
            // - type
            // - out
            // - out degree
            // - in
            // - in degree
            // - loop
            // - loop degree
            this.array = arrayFactory.newDynamicByteArray( chunkSize, DEFAULT_VALUE );
            this.nextFreeId = new AtomicLong( base );
        }

        public int getCount( long id, int type, Direction direction )
        {
            id = findGroupIndexForType( id, type );
            return id == EMPTY ? 0 : array.getInt( rebase( id ), countOffset( direction ) );
        }

        private void clearRelationships()
        {
            long length = array.length();
            for ( long i = 0; i < length; i++ )
            {
                ByteArray array = this.array.at( i );
                array.set6ByteLong( i, directionOffset( Direction.OUTGOING ), EMPTY );
                array.set6ByteLong( i, directionOffset( Direction.INCOMING ), EMPTY );
                array.set6ByteLong( i, directionOffset( Direction.BOTH ), EMPTY );
            }
        }

        /**
         * Compensate for test value of index (to avoid allocating all your RAM)
         */
        private long rebase( long index )
        {
            return index - base;
        }

        private long nextFreeId()
        {
            return nextFreeId.getAndIncrement();
        }

        private void initializeGroup( ByteArray array, long relGroupIndex, int type )
        {
            array.setShort( rebase( relGroupIndex ), TYPE_OFFSET, (short) type );
            // All other values are set to defaults automatically
        }

        private long visitGroups( long nodeId, long relGroupIndex, GroupVisitor visitor )
        {
            long currentIndex = relGroupIndex;
            long first = -1;
            while ( currentIndex != EMPTY )
            {
                long index = rebase( currentIndex );
                ByteArray array = this.array.at( index );
                int type = array.getShort( index, TYPE_OFFSET );
                long out = all48Bits( array, index, directionOffset( Direction.OUTGOING ) );
                long in = all48Bits( array, index, directionOffset( Direction.INCOMING ) );
                long loop = all48Bits( array, index, directionOffset( Direction.BOTH ) );
                long next = all48Bits( array, index, NEXT_OFFSET );
                long id = visitor.visit( nodeId, type, next, out, in, loop );
                if ( first == -1 )
                {   // This is the one we return
                    first = id;
                }

                currentIndex = next;
            }
            return first;
        }

        private int directionOffset( Direction direction )
        {
            return BASE_IDS_OFFSET + (direction.ordinal() * ID_AND_COUNT_SIZE);
        }

        private int countOffset( Direction direction )
        {
            return directionOffset( direction ) + ID_SIZE;
        }

        public long allocate( int type, Direction direction, long relId, boolean incrementCount )
        {
            long index = nextFreeId();
            ByteArray array = this.array.at( rebase( index ) );
            initializeGroup( array, index, type );
            putRelField( array, index, direction, relId, incrementCount );
            return index;
        }

        private long putRelField( ByteArray array, long relGroupIndex, Direction direction,
                long relId, boolean increment )
        {
            long index = rebase( relGroupIndex );
            int directionOffset = directionOffset( direction );
            long previousId = all48Bits( array, index, directionOffset );
            array.set6ByteLong( index, directionOffset, relId );
            if ( increment )
            {
                int countOffset = countOffset( direction );
                array.setInt( index, countOffset, array.getInt( index, countOffset ) + 1 );
            }
            return previousId;
        }

        public long putRelationship( long relGroupIndex, int type, Direction direction, long relId,
                boolean trueForIncrement )
        {
            long currentIndex = relGroupIndex;
            long previousIndex = EMPTY;
            while ( currentIndex != EMPTY )
            {
                long currentIndexRebased = rebase( currentIndex );
                ByteArray array = this.array.at( currentIndexRebased );
                long foundType = array.getShort( currentIndexRebased, TYPE_OFFSET );
                if ( foundType == type )
                {   // Found it
                    return putRelField( array, currentIndex, direction, relId, trueForIncrement );
                }
                else if ( foundType > type )
                {   // We came too far, create room for it
                    break;
                }
                previousIndex = currentIndex;
                currentIndex = all48Bits( array, currentIndexRebased, NEXT_OFFSET );
            }

            long newIndex = nextFreeId();
            if ( previousIndex == EMPTY )
            {   // We are at the start
                array.swap( rebase( currentIndex ), rebase( newIndex ), 1 );
                long swap = newIndex;
                newIndex = currentIndex;
                currentIndex = swap;
            }

            ByteArray array = this.array.at( rebase( newIndex ) );
            initializeGroup( array, newIndex, type );
            if ( currentIndex != EMPTY )
            {   // We are NOT at the end
                setNextField( array, newIndex, currentIndex );
            }

            if ( previousIndex != EMPTY )
            {   // We are NOT at the start
                setNextField( this.array, previousIndex, newIndex );
            }

            return putRelField( array, newIndex, direction, relId, trueForIncrement );
        }

        private void setNextField( ByteArray array, long relGroupIndex, long next )
        {
            array.set6ByteLong( rebase( relGroupIndex ), NEXT_OFFSET, next );
        }

        private long findGroupIndexForType( long relGroupIndex, int type )
        {
            long currentIndex = relGroupIndex;
            while ( currentIndex != EMPTY )
            {
                long index = rebase( currentIndex );
                int foundType = array.getShort( index, TYPE_OFFSET );
                if ( foundType == type )
                {   // Found it
                    return currentIndex;
                }
                else if ( foundType > type )
                {   // We came too far, create room for it
                    break;
                }
                currentIndex = all48Bits( array, index, NEXT_OFFSET );
            }
            return EMPTY;
        }

        @Override
        public void close()
        {
            array.close();
        }

        @Override
        public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
        {
            array.acceptMemoryStatsVisitor( visitor );
        }
    }

    @Override
    public String toString()
    {
        return array.toString();
    }

    public void close()
    {
        array.close();
        relGroupCache.close();
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        array.acceptMemoryStatsVisitor( visitor );
        relGroupCache.acceptMemoryStatsVisitor( visitor );
    }
}
