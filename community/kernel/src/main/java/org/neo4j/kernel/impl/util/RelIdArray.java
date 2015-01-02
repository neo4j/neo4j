/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.cache.SizeOfObject;

import static java.lang.System.arraycopy;

import static org.neo4j.kernel.impl.cache.SizeOfs.withArrayOverhead;
import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;
import static org.neo4j.kernel.impl.cache.SizeOfs.withReference;

public class RelIdArray implements SizeOfObject
{
    private static final DirectionWrapper[] DIRECTIONS_FOR_OUTGOING =
            new DirectionWrapper[] { DirectionWrapper.OUTGOING, DirectionWrapper.BOTH };
    private static final DirectionWrapper[] DIRECTIONS_FOR_INCOMING =
            new DirectionWrapper[] { DirectionWrapper.INCOMING, DirectionWrapper.BOTH };
    private static final DirectionWrapper[] DIRECTIONS_FOR_BOTH =
            new DirectionWrapper[] { DirectionWrapper.OUTGOING, DirectionWrapper.INCOMING, DirectionWrapper.BOTH };

    public static class EmptyRelIdArray extends RelIdArray
    {
        private static final DirectionWrapper[] EMPTY_DIRECTION_ARRAY = new DirectionWrapper[0];
        private final RelIdIterator EMPTY_ITERATOR = new RelIdIteratorImpl( this, EMPTY_DIRECTION_ARRAY )
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

            @Override
            public void doAnotherRound()
            {
            }

            @Override
            public RelIdIterator updateSource( RelIdArray newSource, DirectionWrapper direction )
            {
                return direction.iterator( newSource );
            }
        };

        private EmptyRelIdArray( int type )
        {
            super( type );
        }

        @Override
        public RelIdIterator iterator( final DirectionWrapper direction )
        {
            return EMPTY_ITERATOR;
        }
    };

    public static RelIdArray empty( int type )
    {
        return new EmptyRelIdArray( type );
    }

    public static final RelIdArray EMPTY = new EmptyRelIdArray( -1 );

    private final int type;
    private IdBlock outBlock;
    private IdBlock inBlock;

    public RelIdArray( int type )
    {
        this.type = type;
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        return withObjectOverhead( 8 /*type (padded)*/ + sizeOfBlockWithReference( outBlock ) + sizeOfBlockWithReference( inBlock ) );
    }

    static int sizeOfBlockWithReference( IdBlock block )
    {
        return withReference( block != null ? block.sizeOfObjectInBytesIncludingOverhead() : 0 );
    }

    public int getType()
    {
        return type;
    }

    protected RelIdArray( RelIdArray from )
    {
        this( from.type );
        this.outBlock = from.outBlock;
        this.inBlock = from.inBlock;
    }

    protected RelIdArray( int type, IdBlock out, IdBlock in )
    {
        this( type );
        this.outBlock = out;
        this.inBlock = in;
    }

    /*
     * Adding an id with direction BOTH means that it's a loop
     */
    public void add( long id, DirectionWrapper direction )
    {
        IdBlock block = direction.getBlock( this );
        if ( block == null || !block.accepts( id ) )
        {
            IdBlock newBlock = null;
            if ( block == null && LowIdBlock.idIsLow( id ) )
            {
                newBlock = new LowIdBlock();
            }
            else
            {
                newBlock = block != null ? block.upgradeToHighIdBlock() : new HighIdBlock();
            }
            direction.setBlock( this, newBlock );
            block = newBlock;
        }
        block.add( id );
    }

    protected boolean accepts( RelIdArray source )
    {
        return source.getLastLoopBlock() == null;
    }

    public RelIdArray addAll( RelIdArray source )
    {
//        if ( source == null )
//        {
//            return this;
//        }

        if ( !accepts( source ) )
        {
            return upgradeIfNeeded( source ).addAll( source );
        }
        else
        {
            appendFrom( source, DirectionWrapper.OUTGOING );
            appendFrom( source, DirectionWrapper.INCOMING );
            appendFrom( source, DirectionWrapper.BOTH );
            return this;
        }
    }

    protected IdBlock getLastLoopBlock()
    {
        return null;
    }

    public RelIdArray shrink()
    {
        IdBlock shrunkOut = outBlock != null ? outBlock.shrink() : null;
        IdBlock shrunkIn = inBlock != null ? inBlock.shrink() : null;
        return shrunkOut == outBlock && shrunkIn == inBlock ? this : new RelIdArray( type, shrunkOut, shrunkIn );
    }

    protected void setLastLoopBlock( IdBlock block )
    {
        throw new UnsupportedOperationException( "Should've upgraded to RelIdArrayWithLoops before this" );
    }

    public RelIdArray upgradeIfNeeded( RelIdArray capabilitiesToMatch )
    {
        return capabilitiesToMatch.getLastLoopBlock() != null ? new RelIdArrayWithLoops( this ) : this;
    }

    public RelIdArray downgradeIfPossible()
    {
        return this;
    }

    protected void appendFrom( RelIdArray source, DirectionWrapper direction )
    {
        IdBlock toBlock = direction.getBlock( this );
        IdBlock fromBlock = direction.getBlock( source );
        if ( fromBlock == null )
        {
            return;
        }

        if ( toBlock == null )
        {   // We've got no ids for that direction, just pop it right in (a copy of it)
            direction.setBlock( this, fromBlock.copyAndShrink() );
        }
        else if ( toBlock.accepts( fromBlock ) )
        {   // We've got some existing ids and the new ids are compatible, so add them
            toBlock.addAll( fromBlock );
        }
        else
        {   // We've got some existing ids, but ids aren't compatible. Upgrade and add them to the upgraded block
            toBlock = toBlock.upgradeToHighIdBlock();
            toBlock.addAll( fromBlock );
            direction.setBlock( this, toBlock );
        }
    }

    public boolean isEmpty()
    {
        return outBlock == null && inBlock == null && getLastLoopBlock() == null ;
    }

    public RelIdIterator iterator( DirectionWrapper direction )
    {
        return direction.iterator( this );
    }

    protected RelIdArray newSimilarInstance()
    {
        return new RelIdArray( type );
    }

    public static final IdBlock EMPTY_BLOCK = new LowIdBlock();

    public static enum DirectionWrapper
    {
        OUTGOING( Direction.OUTGOING )
                {
                    @Override
                    RelIdIterator iterator( RelIdArray ids )
                    {
                        return new RelIdIteratorImpl( ids, DIRECTIONS_FOR_OUTGOING );
                    }

                    @Override
                    IdBlock getBlock( RelIdArray ids )
                    {
                        return ids.outBlock;
                    }

                    @Override
                    void setBlock( RelIdArray ids, IdBlock block )
                    {
                        ids.outBlock = block;
                    }
                },
        INCOMING( Direction.INCOMING )
                {
                    @Override
                    RelIdIterator iterator( RelIdArray ids )
                    {
                        return new RelIdIteratorImpl( ids, DIRECTIONS_FOR_INCOMING );
                    }

                    @Override
                    IdBlock getBlock( RelIdArray ids )
                    {
                        return ids.inBlock;
                    }

                    @Override
                    void setBlock( RelIdArray ids, IdBlock block )
                    {
                        ids.inBlock = block;
                    }
                },
        BOTH( Direction.BOTH )
                {
                    @Override
                    RelIdIterator iterator( RelIdArray ids )
                    {
                        return new RelIdIteratorImpl( ids, DIRECTIONS_FOR_BOTH );
                    }

                    @Override
                    IdBlock getBlock( RelIdArray ids )
                    {
                        return ids.getLastLoopBlock();
                    }

                    @Override
                    void setBlock( RelIdArray ids, IdBlock block )
                    {
                        ids.setLastLoopBlock( block );
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
        abstract IdBlock getBlock( RelIdArray ids );

        /*
         * Only used during add
         */
        abstract void setBlock( RelIdArray ids, IdBlock block );

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

    public static abstract class IdBlock implements SizeOfObject
    {
        /**
         * @return a shrunk version of itself. It returns itself if there is
         * no need to shrink it or a {@link #copyAndShrink()} if there is slack in the array.
         */
        IdBlock shrink()
        {
            return length() == capacity() ? this : copyAndShrink();
        }

        void add( long id )
        {
            int length = ensureSpace( 1 );
            set( id, length );
            setLength( length+1 );
        }

        void addAll( IdBlock block )
        {
            int otherBlockLength = block.length();
            int length = ensureSpace( otherBlockLength+1 );
            append( block, length+1, otherBlockLength );
            setLength( otherBlockLength+length );
        }

        /**
         * Returns the number of ids in the array, not the array size.
         */
        int ensureSpace( int delta )
        {
            int length = length();
            int newLength = length+delta;
            int capacity = capacity();
            if ( newLength >= capacity )
            {   // We're out of space, try doubling the size
                int calculatedLength = capacity*2;
                if ( newLength > calculatedLength )
                {   // Doubling the size wasn't enough, go with double what was required
                    calculatedLength = newLength*2;
                }
                extendArrayTo( length, calculatedLength );
            }
            return length;
        }

        protected abstract boolean accepts( long id );

        protected abstract boolean accepts( IdBlock block );

        protected abstract IdBlock copyAndShrink();

        abstract IdBlock upgradeToHighIdBlock();

        protected abstract void extendArrayTo( int numberOfItemsToCopy, int newLength );

        protected abstract void setLength( int length );

        protected abstract int length();

        protected abstract int capacity();

        protected abstract void append( IdBlock source, int targetStartIndex, int itemsToCopy );

        protected abstract long get( int index );

        protected abstract void set( long id, int index );
    }

    private static class LowIdBlock extends IdBlock
    {
        // First element is the actual length w/o the slack
        private int[] ids = new int[3];

        @Override
        public int sizeOfObjectInBytesIncludingOverhead()
        {
            return withObjectOverhead( withReference( withArrayOverhead( 4*ids.length ) ) );
        }

        public static boolean idIsLow( long id )
        {
            return (id & 0xFF00000000L) == 0;
        }

        @Override
        protected boolean accepts( long id )
        {
            return idIsLow( id );
        }

        @Override
        protected boolean accepts( IdBlock block )
        {
            return block instanceof LowIdBlock;
        }

        @Override
        protected void append( IdBlock source, int targetStartIndex, int itemsToCopy )
        {
            if ( source instanceof LowIdBlock )
            {
                arraycopy( ((LowIdBlock)source).ids, 1, ids, targetStartIndex, itemsToCopy );
            }
            else
            {
                throw new IllegalArgumentException( source.toString() );
            }
        }

        @Override
        IdBlock upgradeToHighIdBlock()
        {
            return new HighIdBlock( this );
        }

        @Override
        protected IdBlock copyAndShrink()
        {
            LowIdBlock copy = new LowIdBlock();
            copy.ids = Arrays.copyOf( ids, length()+1 );
            return copy;
        }

        @Override
        protected void extendArrayTo( int numberOfItemsToCopy, int newLength )
        {
            int[] newIds = new int[newLength+1];
            arraycopy( ids, 0, newIds, 0, numberOfItemsToCopy+1 );
            ids = newIds;
        }

        @Override
        protected int length()
        {
            return ids[0];
        }

        @Override
        protected int capacity()
        {
            return ids.length-1;
        }

        @Override
        protected void setLength( int length )
        {
            ids[0] = length;
        }

        @Override
        protected long get( int index )
        {
            assert index >= 0 && index < length();
            return ids[index+1]&0xFFFFFFFFL;
        }

        @Override
        protected void set( long id, int index )
        {
            ids[index+1] = (int) id; // guarded from outside that this is indeed an int
        }
    }

    private static class HighIdBlock extends IdBlock
    {
        // First element is the actual length w/o the slack
        private int[] ids;
        private byte[] highBits;

        public HighIdBlock()
        {
            ids = new int[3];
            highBits = new byte[3];
        }

        private HighIdBlock( LowIdBlock lowIdBlock )
        {
            ids = Arrays.copyOf( lowIdBlock.ids, lowIdBlock.ids.length );
            highBits = new byte[ids.length];
        }

        @Override
        public int sizeOfObjectInBytesIncludingOverhead()
        {
            return withObjectOverhead(
                    withReference( withArrayOverhead( 4*ids.length ) ) +
                            withReference( withArrayOverhead( ids.length ) ) );
        }

        @Override
        protected boolean accepts( long id )
        {
            return true;
        }

        @Override
        protected boolean accepts( IdBlock block )
        {
            return true;
        }

        @Override
        protected void append( IdBlock source, int targetStartIndex, int itemsToCopy )
        {
            if ( source instanceof LowIdBlock )
            {
                arraycopy( ((LowIdBlock)source).ids, 1, ids, targetStartIndex, itemsToCopy );
            }
            else
            {
                arraycopy( ((HighIdBlock)source).ids, 1, ids, targetStartIndex, itemsToCopy );
                arraycopy( ((HighIdBlock)source).highBits, 1, highBits, targetStartIndex, itemsToCopy );
            }
        }

        @Override
        IdBlock upgradeToHighIdBlock()
        {
            return this;
        }

        @Override
        protected IdBlock copyAndShrink()
        {
            HighIdBlock copy = new HighIdBlock();
            int itemsToCopy = length()+1;
            copy.ids = Arrays.copyOf( ids, itemsToCopy );
            copy.highBits = Arrays.copyOf( highBits, itemsToCopy );
            return copy;
        }

        @Override
        protected void extendArrayTo( int numberOfItemsToCopy, int newLength )
        {
            int[] newIds = new int[newLength+1];
            byte[] newHighBits = new byte[newLength+1];
            arraycopy( ids, 0, newIds, 0, numberOfItemsToCopy+1 );
            arraycopy( highBits, 0, newHighBits, 0, numberOfItemsToCopy+1 );
            ids = newIds;
            highBits = newHighBits;
        }

        @Override
        protected int length()
        {
            return ids[0];
        }

        @Override
        protected int capacity()
        {
            return ids.length-1;
        }

        @Override
        protected void setLength( int length )
        {
            ids[0] = length;
        }

        @Override
        protected long get( int index )
        {
            return ((long)highBits[index+1] << 32) | ids[index+1]&0xFFFFFFFFL;
        }

        @Override
        protected void set( long id, int index )
        {
            ids[index+1] = (int)id;
            highBits[index+1] = (byte) ((id&0xFF00000000L) >>> 32);
        }
    }

    private static class IteratorState
    {
        private IdBlock block;
        private int relativePosition;

        public IteratorState( IdBlock block, int relativePosition )
        {
            this.block = block;
            this.relativePosition = relativePosition;
        }

        boolean hasNext()
        {
            return relativePosition < block.length();
        }

        /*
         * Only called if hasNext returns true
         */
        long next()
        {
            long id = block.get( relativePosition++ );
            return id;
        }

        public void update( IdBlock block )
        {
            this.block = block;
        }
    }

    public static class RelIdIteratorImpl implements RelIdIterator
    {
        private final DirectionWrapper[] directions;
        private int directionPosition = -1;
        private DirectionWrapper currentDirection;
        private IteratorState currentState;
        private final IteratorState[] states;

        private long nextElement;
        private boolean nextElementDetermined;
        private RelIdArray ids;

        RelIdIteratorImpl( RelIdArray ids, DirectionWrapper[] directions )
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
                block = currentDirection.getBlock( ids );
            }

            if ( block != null )
            {
                currentState = new IteratorState( block, 0 );
                states[directionPosition] = currentState;
            }
        }

        @Override
        public int getType()
        {
            return ids.getType();
        }

        @Override
        public RelIdIterator updateSource( RelIdArray newSource, DirectionWrapper direction )
        {
            if ( ids != newSource || newSource.couldBeNeedingUpdate() )
            {
                ids = newSource;

                // Blocks may have gotten upgraded to support a linked list
                // of blocks, so reestablish those references.
                for ( int i = 0; i < states.length; i++ )
                {
                    if ( states[i] != null )
                    {
                        states[i].update( directions[i].getBlock( ids ) );
                    }
                }
            }
            return this;
        }

        @Override
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
            while ( directionPosition+1 < directions.length )
            {
                currentDirection = directions[++directionPosition];
                IteratorState nextState = states[directionPosition];
                if ( nextState != null )
                {
                    currentState = nextState;
                    return true;
                }
                IdBlock block = currentDirection.getBlock( ids );
                if ( block != null )
                {
                    currentState = new IteratorState( block, 0 );
                    states[directionPosition] = currentState;
                    return true;
                }
            }
            return false;
        }

        @Override
        public void doAnotherRound()
        {
            directionPosition = -1;
            nextBlock();
        }

        @Override
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

    public static RelIdArray from( RelIdArray src, RelIdArray add, Collection<Long> remove )
    {
        if ( remove == null )
        {
            if ( src == null )
            {
                return add.downgradeIfPossible();
            }
            if ( add != null )
            {
                src = src.addAll( add );
                return src.downgradeIfPossible();
            }
            return src;
        }
        else
        {
            if ( src == null && add == null )
            {
                return null;
            }
            RelIdArray newArray = null;
            if ( src != null )
            {
                newArray = src.newSimilarInstance();
                newArray.addAll( src );
                evictExcluded( newArray, remove );
            }
            else
            {
                newArray = add.newSimilarInstance();
            }
            if ( add != null )
            {
                newArray = newArray.upgradeIfNeeded( add );
                for ( RelIdIteratorImpl fromIterator = (RelIdIteratorImpl) add.iterator( DirectionWrapper.BOTH );
                      fromIterator.hasNext();)
                {
                    long value = fromIterator.next();
                    if ( !remove.contains( value ) )
                    {
                        newArray.add( value, fromIterator.currentDirection );
                    }
                }
            }
            return newArray;
        }
    }

    private static void evictExcluded( RelIdArray ids, Collection<Long> excluded )
    {
        for ( RelIdIteratorImpl iterator = (RelIdIteratorImpl) DirectionWrapper.BOTH.iterator( ids );
              iterator.hasNext(); )
        {
            long value = iterator.next();
            if ( excluded.contains( value ) )
            {
                boolean swapSuccessful = false;
                IteratorState state = iterator.currentState;
                IdBlock block = state.block;
                for ( int j = block.length() - 1; j >= state.relativePosition; j--)
                {
                    long backValue = block.get( j );
                    block.setLength( block.length()-1 );
                    if ( !excluded.contains( backValue) )
                    {
                        block.set( backValue, state.relativePosition-1 );
                        swapSuccessful = true;
                        break;
                    }
                }
                if ( !swapSuccessful ) // all elements from pos in remove
                {
                    block.setLength( block.length()-1 );
                }
            }
        }
    }

    /**
     * Optimization in the lazy loading of relationships for a node.
     * {@link RelIdIterator#updateSource(RelIdArray, org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper)}
     * is only called if this returns true, i.e if a {@link RelIdArray} or {@link IdBlock} might have
     * gotten upgraded to handle f.ex loops or high id ranges so that the
     * {@link RelIdIterator} gets updated accordingly.
     */
    public boolean couldBeNeedingUpdate()
    {
        return (outBlock != null && outBlock instanceof HighIdBlock) ||
                (inBlock != null && inBlock instanceof HighIdBlock);
    }
}