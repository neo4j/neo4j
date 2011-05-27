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
    private static final DirectionWrapper[] DIRECTIONS_FOR_OUTGOING =
            new DirectionWrapper[] { DirectionWrapper.OUTGOING, DirectionWrapper.BOTH };
    private static final DirectionWrapper[] DIRECTIONS_FOR_INCOMING =
            new DirectionWrapper[] { DirectionWrapper.INCOMING, DirectionWrapper.BOTH };
    private static final DirectionWrapper[] DIRECTIONS_FOR_BOTH =
            new DirectionWrapper[] { DirectionWrapper.OUTGOING, DirectionWrapper.INCOMING, DirectionWrapper.BOTH };
    
    public static final RelIdArray EMPTY = new RelIdArray()
    {
        private RelIdIterator emptyIterator = new RelIdIterator( null, new DirectionWrapper[0] )
        {
            @Override
            public boolean hasNext()
            {
                return false;
            }

            @Override
            protected boolean nextBlock()
            {
                return false;
            }
            
            public void doAnotherRound()
            {
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
    private IdBlock lastLoopBlock;
    
    public RelIdArray()
    {
    }
    
    /*
     * Adding an id with direction BOTH means that it's a loop
     */
    public void add( long id, DirectionWrapper direction )
    {
        IdBlock lastBlock = direction.lastBlock( this );
        long highBits = id&0xFFFFFFFF00000000L;
        if ( lastBlock == null || lastBlock.getHighBits() != highBits )
        {
            IdBlock newLastBlock = highBits == 0 ? new LowIdBlock() : new HighIdBlock( highBits );
            newLastBlock.prev = lastBlock;
            direction.assignTopLevelBlock( this, newLastBlock );
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
        
        // TODO cram into DirectionWrapper
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
                if ( source.lastInBlock.prev != null )
                {
                    last( lastInBlock ).prev = source.lastInBlock.prev.copy();
                }
            }
            else
            {
                last( lastInBlock ).prev = source.lastInBlock.copy();
            }
        }
        if ( source.lastLoopBlock != null )
        {
            if ( lastLoopBlock == null )
            {
                lastLoopBlock = source.lastLoopBlock.copy();
            }
            else if ( lastLoopBlock.getHighBits() == source.lastLoopBlock.getHighBits() )
            {
                lastLoopBlock.addAll( source.lastLoopBlock );
                if ( source.lastLoopBlock.prev != null )
                {
                    last( lastLoopBlock ).prev = source.lastLoopBlock.prev.copy();
                }
            }
            else
            {
                last( lastLoopBlock ).prev = source.lastLoopBlock.copy();
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
        return lastOutBlock == null && lastInBlock == null/* && lastLoopBlock == null */;
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
                return new RelIdIterator( ids, DIRECTIONS_FOR_OUTGOING );
            }

            @Override
            IdBlock lastBlock( RelIdArray ids )
            {
                return ids.lastOutBlock;
            }

            @Override
            void assignTopLevelBlock( RelIdArray ids, IdBlock block )
            {
                ids.lastOutBlock = block;
            }
        },
        INCOMING( Direction.INCOMING )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new RelIdIterator( ids, DIRECTIONS_FOR_INCOMING );
            }

            @Override
            IdBlock lastBlock( RelIdArray ids )
            {
                return ids.lastInBlock;
            }

            @Override
            void assignTopLevelBlock( RelIdArray ids, IdBlock block )
            {
                ids.lastInBlock = block;
            }
        },
        BOTH( Direction.BOTH )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new RelIdIterator( ids, DIRECTIONS_FOR_BOTH );
            }

            @Override
            IdBlock lastBlock( RelIdArray ids )
            {
                return ids.lastLoopBlock;
            }

            @Override
            void assignTopLevelBlock( RelIdArray ids, IdBlock block )
            {
                ids.lastLoopBlock = block;
            }
        };
        
        private final Direction direction;

        private DirectionWrapper( Direction direction )
        {
            this.direction = direction;
        }
        
        abstract RelIdIterator iterator( RelIdArray ids );
        
        /*
         * Only used during add
         */
        abstract IdBlock lastBlock( RelIdArray ids );
        
        /*
         * Only used during add
         */
        abstract void assignTopLevelBlock( RelIdArray ids, IdBlock block );
        
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
        default: throw new IllegalArgumentException( "" + direction );
        }
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
    
    private static class IteratorState
    {
        private IdBlock block;
        private int relativePosition;
        private int absolutePosition;
        
        public IteratorState( IdBlock block, int relativePosition )
        {
            this.block = block;
            this.relativePosition = relativePosition;
        }
        
        boolean nextBlock()
        {
            if ( block.prev != null )
            {
                block = block.prev;
                relativePosition = 0;
                return true;
            }
            return false;
        }
        
        boolean hasNext()
        {
            return relativePosition < block.length;
        }
        
        /*
         * Only called if hasNext returns true
         */
        long next()
        {
            absolutePosition++;
            return block.get( relativePosition++ );
        }
    }
    
    public static class RelIdIterator
    {
        private final DirectionWrapper[] directions;
        private int directionPosition = -1;
        private DirectionWrapper currentDirection;
        private IteratorState currentState;
        private final IteratorState[] states;
        
        private long nextElement;
        private boolean nextElementDetermined;
        private final RelIdArray ids;
        
        RelIdIterator( RelIdArray ids, DirectionWrapper[] directions )
        {
            this.ids = ids;
            this.directions = directions;
            this.states = new IteratorState[directions.length];
            
            // Find the initial block which isn't null. There can be directions
            // which have a null block currently, but could potentially be set
            // after the next getMoreRelationships.
            IdBlock block = null;
            while ( block == null && directionPosition+1 < directions.length )
            {
                currentDirection = directions[++directionPosition];
                block = currentDirection.lastBlock( ids );
            }
            
            if ( block != null )
            {
                currentState = new IteratorState( block, 0 );
                states[directionPosition] = currentState;
            }
        }
        
        public boolean hasNext()
        {
            if ( nextElementDetermined )
            {
                return nextElement != -1;
            }
            
            while ( true )
            {
                if ( currentState != null && currentState.hasNext() )
                {
                    nextElement = currentState.next();
                    nextElementDetermined = true;
                    return true;
                }
                else
                {
                    if ( !nextBlock() )
                    {
                        break;
                    }
                }
            }
            
            // Keep this false since the next call could come after we've loaded
            // some more relationships
            nextElementDetermined = false;
            nextElement = -1;
            return false;
        }

        protected boolean nextBlock()
        {
            // Try next block in the chain
            if ( currentState != null && currentState.nextBlock() )
            {
                return true;
            }
            
            // It's ok to return null here... which will result in hasNext
            // returning false. IntArrayIterator will try to get more relationships
            // and call hasNext again.
            return findNextBlock();
        }
        
        /**
         * Tells this iterator to try another round with all its directions
         * starting from each their previous states. Called from IntArrayIterator,
         * when it finds out it has gotten more relationships of this type.
         */
        public void doAnotherRound()
        {
            directionPosition = -1;
            findNextBlock();
        }

        protected boolean findNextBlock()
        {
            while ( directionPosition+1 < directions.length )
            {
                currentDirection = directions[++directionPosition];
                IteratorState nextState = states[directionPosition];
                if ( nextState != null )
                {
                    currentState = nextState;
                    return true;
                }
                IdBlock block = currentDirection.lastBlock( ids );
                if ( block != null )
                {
                    currentState = new IteratorState( block, 0 );
                    states[directionPosition] = currentState;
                    return true;
                }
            }
            return false;
        }
        
        public long next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException();
            }
            nextElementDetermined = false;
            return nextElement;
        }
    }
    
//    private static class SingleBlockRelIdIterator extends RelIdIterator
//    {
//        SingleBlockRelIdIterator( RelIdArray ids, DirectionWrapper[] directions )
//        {
//            super( ids, directions );
//        }
//        
//        @Override
//        protected boolean findNextBlock()
//        {
//            return false;
//        }
//    }
    
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
                for ( RelIdIterator fromIterator = add.iterator( DirectionWrapper.BOTH ); fromIterator.hasNext();)
                {
                    long value = fromIterator.next();
                    if ( !removedSet.contains( value ) )
                    {
                        newArray.add( value, fromIterator.currentDirection );
                    }
                }
            }
            return newArray;
        }
    }

//    private static void addOneDirection( RelIdIterator fromIterator, RelIdArray toArray,
//            Set<Long> removedSet, DirectionWrapper addDirection )
//    {
//        while ( fromIterator.hasNext() )
//        {
//            long value = fromIterator.next();
//            if ( !removedSet.contains( value ) )
//            {
//                toArray.add( value, addDirection );
//            }
//        }
//    }

    private static void evictExcluded( RelIdArray ids, Set<Long> excluded )
    {
        for ( RelIdIterator iterator = DirectionWrapper.BOTH.iterator( ids ); iterator.hasNext(); )
        {
            long value = iterator.next();
            if ( excluded.contains( value ) )
            {
                boolean swapSuccessful = false;
                IteratorState state = iterator.currentState;
                IdBlock block = state.block;
                for ( int j = block.length - 1; j >= state.relativePosition; j--)
                {
                    long backValue = block.get( j );
                    block.length--;
                    if ( !excluded.contains( backValue) )
                    {
                        block.set( backValue, state.relativePosition-1 );
                        swapSuccessful = true;
                        break;
                    }
                }
                if ( !swapSuccessful ) // all elements from pos in remove
                {
                    block.length--;
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
