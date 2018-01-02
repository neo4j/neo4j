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

    private LongArray array;
    private final int denseNodeThreshold;
    private final RelGroupCache relGroupCache;
    private final long base;

    public NodeRelationshipCache( NumberArrayFactory arrayFactory, int denseNodeThreshold )
    {
        this( arrayFactory, denseNodeThreshold, 0 );
    }

    NodeRelationshipCache( NumberArrayFactory arrayFactory, int denseNodeThreshold, long base )
    {
        int chunkSize = 1_000_000;
        this.array = arrayFactory.newDynamicLongArray( chunkSize, IdFieldManipulator.emptyField() );
        this.denseNodeThreshold = denseNodeThreshold;
        this.base = base;
        this.relGroupCache = new RelGroupCache( arrayFactory, chunkSize, base );
    }

    /**
     * Increment relationship count for {@code nodeId}.
     * @param nodeId node to increment relationship count for.
     * @return count after the increment.
     */
    public int incrementCount( long nodeId )
    {
        long field = array.get( nodeId );
        field = IdFieldManipulator.changeCount( field, 1 );
        array.set( nodeId, field );
        return IdFieldManipulator.getCount( field );
    }

    public boolean isDense( long nodeId )
    {
        return fieldIsDense( array.get( nodeId ) );
    }

    private boolean fieldIsDense( long field )
    {
        if ( denseNodeThreshold == EMPTY )
        {   // We haven't initialized the rel group cache yet
            return false;
        }

        return IdFieldManipulator.getCount( field ) >= denseNodeThreshold;
    }

    public long getAndPutRelationship( long nodeId, int type, Direction direction, long firstRelId,
            boolean incrementCount )
    {
        /*
         * OK so the story about counting goes: there's an initial pass for counting number of relationships
         * per node, globally, not per type/direction. After that the relationship group cache is initialized
         * and the relationship stage is executed where next pointers are constructed. That forward pass should
         * not increment the global count, but it should increment the type/direction counts.
         */

        long field = array.get( nodeId );
        long existingId = IdFieldManipulator.getId( field );
        if ( fieldIsDense( field ) )
        {
            if ( existingId == EMPTY )
            {
                existingId = relGroupCache.allocate( type, direction, firstRelId, incrementCount );
                field = IdFieldManipulator.setId( field, existingId );
                array.set( nodeId, field );
                return EMPTY;
            }
            return relGroupCache.putRelationship( existingId, type, direction, firstRelId, incrementCount );
        }

        field = IdFieldManipulator.setId( field, firstRelId );
        // Don't increment count for sparse node since that has already been done in a previous pass
        array.set( nodeId, field );
        return existingId;
    }

    /**
     * Used when setting node nextRel fields. Gets the first relationship for this node,
     * or the first relationship group id (where it it first visits all the groups before returning the first one).
     */
    public long getFirstRel( long nodeId, GroupVisitor visitor )
    {
        long field = array.get( nodeId );
        if ( fieldIsDense( field ) )
        {   // Indirection into rel group cache
            long relGroupIndex = IdFieldManipulator.getId( field );
            return relGroupCache.visitGroups( nodeId, relGroupIndex, visitor );
        }

        return IdFieldManipulator.getId( field );
    }

    public void clearRelationships()
    {
        long length = array.length();
        for ( long i = 0; i < length; i++ )
        {
            long field = array.get( i );
            if ( !fieldIsDense( field ) )
            {
                field = IdFieldManipulator.cleanId( field );
                array.set( i, field );
            }
        }
        relGroupCache.clearRelationships();
    }

    public int getCount( long nodeId, int type, Direction direction )
    {
        long field = array.get( nodeId );
        if ( fieldIsDense( field ) )
        {   // Indirection into rel group cache
            long relGroupIndex = IdFieldManipulator.getId( field );
            if ( relGroupIndex == EMPTY )
            {
                return 0;
            }
            relGroupIndex = relGroupCache.findGroupIndexForType( relGroupIndex, type );
            if ( relGroupIndex == EMPTY )
            {
                return 0;
            }
            field = relGroupCache.getField( relGroupIndex, relGroupCache.directionIndex( direction ) );
        }

        return IdFieldManipulator.getCount( field );
    }

    public void fixateNodes()
    {
        array = array.fixate();
    }

    public void fixateGroups()
    {
        relGroupCache.fixate();
    }

    public interface GroupVisitor
    {
        /**
         * @param nodeId
         * @return the relationship group id created.
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
        private static final int ENTRY_SIZE = 4;

        private static final int INDEX_NEXT_AND_TYPE = 0;
        private static final int INDEX_OUT = 1;
        private static final int INDEX_IN = 2;
        private static final int INDEX_LOOP = 3;

        // Used for testing high id values. Should always be zero in production
        private long base;
        private LongArray array;
        private final AtomicLong nextFreeId;

        RelGroupCache( NumberArrayFactory arrayFactory, long chunkSize, long base )
        {
            this.base = base;
            assert chunkSize > 0;
            this.array = arrayFactory.newDynamicLongArray( chunkSize, -1 );
            this.nextFreeId = new AtomicLong( base );
        }

        private void clearRelationships()
        {
            long length = array.length() / ENTRY_SIZE;
            for ( long i = 0; i < length; i++ )
            {
                clearRelationshipId( i, INDEX_OUT );
                clearRelationshipId( i, INDEX_IN );
                clearRelationshipId( i, INDEX_LOOP );
            }
        }

        private void clearRelationshipId( long relGroupIndex, int fieldIndex )
        {
            long index = index( relGroupIndex, fieldIndex );
            array.set( rebase( index ), IdFieldManipulator.cleanId( array.get( index ) ) );
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

        private void initializeGroup( long relGroupIndex, int type )
        {
            setField( relGroupIndex, INDEX_NEXT_AND_TYPE, NextFieldManipulator.initialFieldWithType( type ) );
            setField( relGroupIndex, INDEX_OUT, IdFieldManipulator.emptyField() );
            setField( relGroupIndex, INDEX_IN, IdFieldManipulator.emptyField() );
            setField( relGroupIndex, INDEX_LOOP, IdFieldManipulator.emptyField() );
        }

        private long visitGroups( long nodeId, long relGroupIndex, GroupVisitor visitor )
        {
            long currentIndex = relGroupIndex;
            long first = -1;
            while ( currentIndex != EMPTY )
            {
                int type = NextFieldManipulator.getType( getField( currentIndex, INDEX_NEXT_AND_TYPE ) );
                long out = IdFieldManipulator.getId( getField( currentIndex, INDEX_OUT ) );
                long in = IdFieldManipulator.getId( getField( currentIndex, INDEX_IN ) );
                long loop = IdFieldManipulator.getId( getField( currentIndex, INDEX_LOOP ) );
                long next = NextFieldManipulator.getNext( getField( currentIndex, INDEX_NEXT_AND_TYPE ) );
                long id = visitor.visit( nodeId, type, next, out, in, loop );
                if ( first == -1 )
                {   // This is the one we return
                    first = id;
                }

                currentIndex = next;
            }
            return first;
        }

        private void setField( long relGroupIndex, int index, long field )
        {
            array.set( index( relGroupIndex, index ), field );
        }

        private long getField( long relGroupIndex, int index )
        {
            return array.get( index( relGroupIndex, index ) );
        }

        private int directionIndex( Direction direction )
        {
            return direction.ordinal()+1;
        }

        private long index( long relGroupIndex, int fieldIndex )
        {
            return rebase( relGroupIndex ) * ENTRY_SIZE + fieldIndex;
        }

        public long allocate( int type, Direction direction, long relId, boolean incrementCount )
        {
            long logicalPosition = nextFreeId();
            initializeGroup( logicalPosition, type );
            putRelField( logicalPosition, direction, relId, incrementCount );
            return logicalPosition;
        }

        private long putRelField( long relGroupIndex, Direction direction, long relId, boolean increment )
        {
            int directionIndex = directionIndex( direction );
            long field = getField( relGroupIndex, directionIndex );
            long previousId = IdFieldManipulator.getId( field );
            field = IdFieldManipulator.setId( field, relId );
            if ( increment )
            {
                field = IdFieldManipulator.changeCount( field, 1 );
            }
            setField( relGroupIndex, directionIndex, field );
            return previousId;
        }

        public long putRelationship( long relGroupIndex, int type, Direction direction, long relId,
                boolean trueForIncrement )
        {
            long currentIndex = relGroupIndex;
            long previousIndex = EMPTY;
            while ( currentIndex != EMPTY )
            {
                long foundType = NextFieldManipulator.getType( getField( currentIndex, INDEX_NEXT_AND_TYPE ) );
                if ( foundType == type )
                {   // Found it
                    return putRelField( currentIndex, direction, relId, trueForIncrement );
                }
                else if ( foundType > type )
                {   // We came too far, create room for it
                    break;
                }
                previousIndex = currentIndex;
                currentIndex = NextFieldManipulator.getNext( getField( currentIndex, INDEX_NEXT_AND_TYPE ) );
            }

            long newIndex = nextFreeId();
            if ( previousIndex == EMPTY )
            {   // We are at the start
                array.swap( index( currentIndex, 0 ), index( newIndex, 0 ), ENTRY_SIZE );
                long swap = newIndex;
                newIndex = currentIndex;
                currentIndex = swap;
            }

            initializeGroup( newIndex, type );
            if ( currentIndex != EMPTY )
            {   // We are NOT at the end
                setNextField( newIndex, currentIndex );
            }

            if ( previousIndex != EMPTY )
            {   // We are NOT at the start
                setNextField( previousIndex, newIndex );
            }

            return putRelField( newIndex, direction, relId, trueForIncrement );
        }

        private void setNextField( long relGroupIndex, long next )
        {
            long field = getField( relGroupIndex, INDEX_NEXT_AND_TYPE );
            field = NextFieldManipulator.setNext( field, next );
            setField( relGroupIndex, INDEX_NEXT_AND_TYPE, field );
        }

        private long findGroupIndexForType( long relGroupIndex, int type )
        {
            long currentIndex = relGroupIndex;
            while ( currentIndex != EMPTY )
            {
                int foundType = NextFieldManipulator.getType( getField( currentIndex, INDEX_NEXT_AND_TYPE ) );
                if ( foundType == type )
                {   // Found it
                    return currentIndex;
                }
                else if ( foundType > type )
                {   // We came too far, create room for it
                    break;
                }
                currentIndex = NextFieldManipulator.getNext( getField( currentIndex, INDEX_NEXT_AND_TYPE ) );
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

        void fixate()
        {
            array = array.fixate();
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
