/*
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
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.cache.SizeOfObject;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

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
        return addAll( source, RelationshipFilter.ACCEPT_ALL );
    }

    public RelIdArray addAll( RelIdArray source, RelationshipFilter filter )
    {
        if ( !accepts( source ) )
        {
            return upgradeIfNeeded( source ).addAll( source, filter );
        }
        else
        {
            appendFrom( source, DirectionWrapper.OUTGOING, filter );
            appendFrom( source, DirectionWrapper.INCOMING, filter );
            appendFrom( source, DirectionWrapper.BOTH, filter );
            return this;
        }
    }

    protected IdBlock getLastLoopBlock()
    {
        return null;
    }

    public void shrink()
    {
        shrink( outBlock );
        shrink( inBlock );
        shrink( getLastLoopBlock() );
    }

    protected void shrink( IdBlock block )
    {
        if ( block != null )
        {
            block.shrink();
        }
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

    protected void appendFrom( RelIdArray source, DirectionWrapper direction, RelationshipFilter filter )
    {
        IdBlock fromBlock = direction.getBlock( source );
        if ( fromBlock == null || !filter.accept( type, direction, direction.firstId( this ) ) )
        {
            return;
        }

        IdBlock toBlock = direction.getBlock( this );
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

    public static enum DirectionWrapper
    {
        OUTGOING( Direction.OUTGOING )
        {
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

            @Override
            public long getNextRel( RelationshipGroupRecord group )
            {
                return group.getFirstOut();
            }

            @Override
            public void setNextRel( RelationshipGroupRecord group, long firstNextRel )
            {
                group.setFirstOut( firstNextRel );
            }

            @Override
            public DirectionWrapper[] allDirections()
            {
                return DIRECTIONS_FOR_OUTGOING;
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

            @Override
            public long getNextRel( RelationshipGroupRecord group )
            {
                return group.getFirstIn();
            }

            @Override
            public void setNextRel( RelationshipGroupRecord group, long firstNextRel )
            {
                group.setFirstIn( firstNextRel );
            }

            @Override
            public DirectionWrapper[] allDirections()
            {
                return DIRECTIONS_FOR_INCOMING;
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

            @Override
            public long getNextRel( RelationshipGroupRecord group )
            {
                return group.getFirstLoop();
            }

            @Override
            public void setNextRel( RelationshipGroupRecord group, long firstNextRel )
            {
                group.setFirstLoop( firstNextRel );
            }

            @Override
            public DirectionWrapper[] allDirections()
            {
                return DIRECTIONS_FOR_BOTH;
            }
        };

        private final Direction direction;

        private DirectionWrapper( Direction direction )
        {
            this.direction = direction;
        }

        public long firstId( RelIdArray ids )
        {
            IdBlock block = getBlock( ids );
            return block != null && block.length() > 0 ? block.get( 0 ) : Record.NO_NEXT_RELATIONSHIP.intValue();
        }

        RelIdIterator iterator( RelIdArray ids )
        {
            return new RelIdIteratorImpl( ids, allDirections() );
        }

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

        public abstract long getNextRel( RelationshipGroupRecord group );

        public abstract void setNextRel( RelationshipGroupRecord group, long firstNextRel );

        public abstract DirectionWrapper[] allDirections();
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
        abstract void shrink();

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

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder( "[" );
            int len = length();
            for ( int i = 0; i < len; i++ )
            {
                if ( i > 0 )
                {
                    builder.append( "," );
                    if ( i % 10 == 0 )
                    {
                        builder.append( "\n" );
                    }
                }
                builder.append( get( i ) );
            }
            return builder.append( "]" ).toString();
        }
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
        void shrink()
        {
            if ( capacity() > length() )
            {
                ids = Arrays.copyOf( ids, length()+1 );
            }
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
            assert index >= 0 && index < length() : "Tried to get an item at index " + index + ", but only allowed indexes are 0-" + length() + "(excl)";
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
        void shrink()
        {
            int itemsToCopy = length()+1;
            ids = Arrays.copyOf( ids, itemsToCopy );
            highBits = Arrays.copyOf( highBits, itemsToCopy );
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
        private int length;

        public IteratorState( IdBlock block, int relativePosition )
        {
            this.block = block;
            this.relativePosition = relativePosition;
            this.length = block.length();
        }

        boolean hasNext()
        {
            return relativePosition < length;
        }

        /*
         * Only called if hasNext returns true
         */
        long next()
        {
            return block.get( relativePosition++ );
        }

        public void update( IdBlock block )
        {
            this.block = block;
            this.length = block.length();
        }
    }

    public static class RelIdIteratorImpl implements RelIdIterator
    {
        private final DirectionWrapper[] directions;
        private byte directionPosition = -1;
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

    public static RelIdArray from( RelIdArray src, RelIdArray add, PrimitiveLongSet remove )
    {
        return from( src, add, remove, RelationshipFilter.ACCEPT_ALL );
    }

    public static RelIdArray from( RelIdArray src, RelIdArray add, PrimitiveLongSet remove,
            RelationshipFilter filter )
    {
        if ( remove == null )
        {
            if ( src == null )
            {
                assert add != null;

                // We create a new array here since we always want to go through the filter
                // and addAll in this case will be a simple "setBlock", no copy, so that's fine
                RelIdArray newArray = add.downgradeIfPossible().newSimilarInstance();
                newArray.addAll( add, filter );
                return newArray;
            }
            if ( add != null )
            {
                src = src.addAll( add, filter );
                return src.downgradeIfPossible();
            }
            return src;
        }

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
            evictExcluded( add, remove );
            newArray.addAll( add, filter );
        }
        return newArray;
    }

    private static void evictExcluded( RelIdArray ids, PrimitiveLongSet excluded )
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
                    state.length--; // state has a cached block length, so change that too
                    if ( !excluded.contains( backValue ) )
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

    public int length( DirectionWrapper dir )
    {
        int result = 0;
        for ( DirectionWrapper direction : dir.allDirections() )
        {
            IdBlock block = direction.getBlock( this );
            if ( block != null )
            {
                result += block.length();
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        IdBlock loopBlock = DirectionWrapper.BOTH.getBlock( this );
        return "RelIdArray for type " + type + ":\n" +
                "  out: " + (outBlock != null ? outBlock.toString() : "" ) + "\n" +
                "  in:  " + (inBlock != null ? inBlock.toString() : "" ) + "\n" +
                "  loop:" + (loopBlock != null ? loopBlock.toString() : "" ) + "\n";
    }
}
