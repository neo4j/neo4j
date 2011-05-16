/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.graphdb.Direction;

public class RelIdArray
{
    public static final RelIdArray EMPTY = new RelIdArray()
    {
        private RelIdIterator emptyIterator = new RelIdIterator( null )
        {
            @Override
            public boolean hasNext()
            {
                return false;
            }

            @Override
            protected IdBlock selectNextBlock( IdBlock currentBlock )
            {
                return null;
            }
        };
        
        @Override
        public RelIdIterator iterator( DirectionWrapper direction )
        {
            return emptyIterator;
        }
    };
    
    private IdBlock lastOutBlock;
    private IdBlock lastInBlock;
    
    public RelIdArray()
    {
    }
    
    public void add( long id, Direction direction )
    {
        // TODO Optimize
        IdBlock lastBlock = direction == Direction.OUTGOING ? lastOutBlock : lastInBlock;
        long highBits = id&0xFFFFFFFF00000000L;
        if ( lastBlock == null || lastBlock.getHighBits() != highBits )
        {
            IdBlock newLastBlock = highBits == 0 ? new LowIdBlock() : new HighIdBlock( highBits );
            newLastBlock.prev = lastBlock;
            if ( direction == Direction.OUTGOING ) lastOutBlock = newLastBlock;
            else lastInBlock = newLastBlock;
            lastBlock = newLastBlock;
        }
        lastBlock.add( (int) id );
    }
    
    public void addAll( RelIdArray source )
    {
        if ( source == null )
        {
            return;
        }
        
        if ( source.lastOutBlock != null )
        {
            if ( lastOutBlock == null )
            {
                lastOutBlock = source.lastOutBlock.copy();
            }
            else if ( lastOutBlock.getHighBits() == source.lastOutBlock.getHighBits() )
            {
                lastOutBlock.addAll( source.lastOutBlock );
                if ( source.lastOutBlock.prev != null )
                {
                    last( lastOutBlock ).prev = source.lastOutBlock.prev.copy();
                }
            }
            else
            {
                last( lastOutBlock ).prev = source.lastOutBlock.copy();
            }
        }
        if ( source.lastInBlock != null )
        {
            if ( lastInBlock == null )
            {
                lastInBlock = source.lastInBlock.copy();
            }
            else if ( lastInBlock.getHighBits() == source.lastInBlock.getHighBits() )
            {
                lastInBlock.addAll( source.lastInBlock );
            }
            else
            {
                last( lastInBlock ).prev = source.lastInBlock.copy();
            }
        }
    }
    
    private static IdBlock last( IdBlock block )
    {
        while ( block.prev != null )
        {
            block = block.prev;
        }
        return block;
    }
    
    public boolean isEmpty()
    {
        return lastOutBlock == null && lastInBlock == null;
    }
    
    public RelIdIterator iterator( DirectionWrapper direction )
    {
        return direction.iterator( this );
    }
    
    public static final IdBlock EMPTY_BLOCK = new LowIdBlock()
    {
        @Override
        int length()
        {
            return 0;
        }
    };
    
    public static enum DirectionWrapper
    {
        OUTGOING( Direction.OUTGOING )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new OneDirectionRelIdIterator( ids.lastOutBlock );
            }
        },
        INCOMING( Direction.INCOMING )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new OneDirectionRelIdIterator( ids.lastInBlock );
            }
        },
        BOTH( Direction.BOTH )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new BothDirectionsRelIdIterator( ids.lastOutBlock, ids.lastInBlock );
            }
        };
        
        private final Direction direction;

        private DirectionWrapper( Direction direction )
        {
            this.direction = direction;
        }
        
        abstract RelIdIterator iterator( RelIdArray ids );
        
        public Direction direction()
        {
            return this.direction;
        }
    }
    
    public static DirectionWrapper wrap( Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING: return DirectionWrapper.OUTGOING;
        case INCOMING: return DirectionWrapper.INCOMING;
        case BOTH: return DirectionWrapper.BOTH;
        }
        throw new IllegalArgumentException( "" + direction );
    }
    
    public static abstract class IdBlock
    {
        private int[] ids = new int[2];
        private int length;
        private IdBlock prev;
        
        IdBlock copy()
        {
            IdBlock copy = copyInstance();
            copy.ids = new int[ids.length];
            System.arraycopy( ids, 0, copy.ids, 0, length );
            copy.length = length;
            if ( prev != null )
            {
                copy.prev = prev.copy();
            }
            return copy;
        }
        
        protected abstract IdBlock copyInstance();

        int length()
        {
            return length;
        }
        
        // Assume id has same high bits
        void add( int id )
        {
            if ( length == ids.length )
            {
                int[] newIds = new int[length*2];
                System.arraycopy( ids, 0, newIds, 0, ids.length );
                ids = newIds;
            }
            ids[length++] = id;
        }
        
        void addAll( IdBlock block )
        {
            int newLength = length + block.length;
            if ( newLength >= ids.length )
            {
                int[] newIds = new int[newLength];
                System.arraycopy( ids, 0, newIds, 0, length );
                ids = newIds;
            }
            System.arraycopy( block.ids, 0, ids, length, block.length );
            length = newLength;
        }
        
        long get( int index )
        {
            assert index >= 0 && index < length;
            return transform( ids[index] );
        }
        
        abstract long transform( int id );
        
        void set( long id, int index )
        {
            // Assume same high bits
            ids[index] = (int) id;
        }
        
        abstract long getHighBits();
    }
    
    private static class LowIdBlock extends IdBlock
    {
        @Override
        long transform( int id )
        {
            return (long)(id&0xFFFFFFFFL);
        }
        
        @Override
        protected IdBlock copyInstance()
        {
            return new LowIdBlock();
        }
        
        @Override
        long getHighBits()
        {
            return 0;
        }
    }
    
    private static class HighIdBlock extends IdBlock
    {
        private final long highBits;

        HighIdBlock( long highBits )
        {
            this.highBits = highBits;
        }
        
        @Override
        long transform( int id )
        {
            return (((long)(id&0xFFFFFFFFL))|(highBits));
        }
        
        @Override
        protected IdBlock copyInstance()
        {
            return new HighIdBlock( highBits );
        }
        
        @Override
        long getHighBits()
        {
            return highBits;
        }
    }
    
    public static abstract class RelIdIterator
    {
        protected IdBlock currentBlock;
        private int relativePosition;
        private int absolutePosition;
        private long nextElement;
        private boolean nextElementDetermined;
        
        RelIdIterator( IdBlock startBlock )
        {
            currentBlock = startBlock;
        }
        
        public boolean hasNext()
        {
            if ( nextElementDetermined )
            {
                return nextElement != -1;
            }
            
            while ( currentBlock != null )
            {
                if ( relativePosition < currentBlock.length() )
                {
                    nextElement = currentBlock.get( relativePosition++ );
                    nextElementDetermined = true;
                    return true;
                }
                else
                {
                    IdBlock nextBlock = selectNextBlock( currentBlock );
                    if ( nextBlock == null )
                    {
                        break;
                    }
                    int leftInBlock = currentBlock.length-relativePosition;
                    currentBlock = nextBlock;
                    relativePosition = 0;
                    absolutePosition += leftInBlock;
                }
            }
            
            // Keep this false since the next call could come after we've loaded
            // some more relationships
            nextElementDetermined = false;
            nextElement = -1;
            return false;
        }
        
        protected abstract IdBlock selectNextBlock( IdBlock currentBlock );

        public long next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException();
            }
            nextElementDetermined = false;
            return nextElement;
        }
        
        public void fastForwardTo( int position )
        {
            while ( this.absolutePosition < position )
            {
                int leftInBlock = currentBlock.length-relativePosition;
                currentBlock = selectNextBlock( currentBlock );
                relativePosition = 0;
                absolutePosition += leftInBlock;
            }
            this.absolutePosition = position;
        }
        
        public int position()
        {
            return absolutePosition;
        }
    }
    
    private static class OneDirectionRelIdIterator extends RelIdIterator
    {
        OneDirectionRelIdIterator( IdBlock startBlock )
        {
            super( startBlock );
        }
        
        @Override
        protected IdBlock selectNextBlock( IdBlock currentBlock )
        {
            return currentBlock.prev;
        }
    }
    
    private static class BothDirectionsRelIdIterator extends RelIdIterator
    {
        private boolean in;
        private final IdBlock inBlock;
        
        BothDirectionsRelIdIterator( IdBlock outBlock, IdBlock inBlock )
        {
            super( outBlock != null ? outBlock : (inBlock != null ? inBlock : EMPTY_BLOCK) );
            this.inBlock = inBlock;
            in = currentBlock == inBlock;
        }
        
        @Override
        protected IdBlock selectNextBlock( IdBlock currentBlock )
        {
            IdBlock result = currentBlock.prev;
            if ( !in && result == null )
            {
                in = true;
                result = inBlock;
            }
            return result;
        }
    }
    
    public static RelIdArray from( RelIdArray src, RelIdArray add, RelIdArray remove )
    {
        if ( remove == null )
        {
            if ( src == null )
            {
                return add;
            }
            if ( add != null )
            {
                RelIdArray newArray = new RelIdArray();
                newArray.addAll( src );
                newArray.addAll( add );
                return newArray;
            }
            return src;
        }
        else
        {
            if ( src == null && add == null )
            {
                return null;
            }
            RelIdArray newArray = new RelIdArray();
            newArray.addAll( src );
            Set<Long> removedSet = remove.asSet();
            evictExcluded( newArray, removedSet );
            if ( add != null )
            {
                addOneDirection( add, newArray, removedSet, DirectionWrapper.OUTGOING );
                addOneDirection( add, newArray, removedSet, DirectionWrapper.INCOMING );
            }
            return newArray;
        }
    }

    private static void addOneDirection( RelIdArray add, RelIdArray newArray, Set<Long> removedSet,
            DirectionWrapper direction )
    {
        for ( RelIdIterator iterator = direction.iterator( add ); iterator.hasNext(); )
        {
            long value = iterator.next();
            if ( !removedSet.contains( value ) )
            {
                newArray.add( value, direction.direction() );
            }
        }
    }

    private static void evictExcluded( RelIdArray ids, Set<Long> excluded )
    {
        for ( RelIdIterator iterator = DirectionWrapper.BOTH.iterator( ids ); iterator.hasNext(); )
        {
            long value = iterator.next();
            if ( excluded.contains( value ) )
            {
                boolean swapSuccessful = false;
                IdBlock block = iterator.currentBlock;
                for ( int j = block.length - 1; j >= iterator.relativePosition; j--)
                {
                    long backValue = block.get( j );
                    block.length--;
                    if ( !excluded.contains( backValue) )
                    {
                        block.set( backValue, iterator.relativePosition-1 );
                        swapSuccessful = true;
                        break;
                    }
                }
                if ( !swapSuccessful ) // all elements from pos in remove
                {
                    iterator.currentBlock.length--;
                }
            }
        }
    }
    
    private Set<Long> asSet()
    {
        Set<Long> set = new HashSet<Long>();
        for ( RelIdIterator iterator = DirectionWrapper.BOTH.iterator( this ); iterator.hasNext(); )
        {
            set.add( iterator.next() );
        }
        return set;
    }
}
