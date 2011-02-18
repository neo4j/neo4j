package org.neo4j.kernel.impl.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class RelIdArray
{
    public static final RelIdArray EMPTY = new RelIdArray()
    {
        private RelIdIterator emptyIterator = new RelIdIterator()
        {
            @Override
            public boolean hasNext()
            {
                return false;
            }
        };
        
        @Override
        public RelIdIterator iterator()
        {
            return emptyIterator;
        }
    };
    
    private final List<IdBlock> blocks = new ArrayList<IdBlock>();
    private IdBlock lastBlock;
    
    public RelIdArray()
    {
    }
    
    public void add( long id )
    {
        long highBits = id&0xFFFFFFFF00000000L;
        if ( lastBlock == null || lastBlock.highBits != highBits )
        {
            addBlock( highBits );
        }
        lastBlock.add( (int) id );
    }
    
    public void addAll( RelIdArray source )
    {
        if ( source == null )
        {
            return;
        }
        
        for ( IdBlock block : source.blocks )
        {
            IdBlock copy = block.copy();
            blocks.add( copy );
            lastBlock = copy;
        }
    }
    
    public int length()
    {
        int length = 0;
        for ( IdBlock block : blocks )
        {
            length += block.length;
        }
        return length;
    }
    
    public boolean isEmpty()
    {
        return blocks.isEmpty();
    }

    private void addBlock( long highBits )
    {
        lastBlock = new IdBlock( highBits );
        blocks.add( lastBlock );
    }
    
    public RelIdIterator iterator()
    {
        return new RelIdIterator();
    }
    
    public static final IdBlock EMPTY_BLOCK = new IdBlock( 0 )
    {
        @Override
        int length()
        {
            return 0;
        }
    };
    
    public static class IdBlock
    {
        private final long highBits;
        private int[] ids;
        private int length;
        
        IdBlock( long highBits )
        {
            this.highBits = highBits;
            this.ids = new int[2];
        }
        
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
        
        long get( int index )
        {
            assert index >= 0 && index < length;
            return (long)ids[index]|highBits;
        }
        
        void set( long id, int index )
        {
            // Assume same high bits
            ids[index] = (int) id;
        }
        
        IdBlock copy()
        {
            IdBlock copy = new IdBlock( highBits );
            copy.ids = new int[length];
            System.arraycopy( ids, 0, copy.ids, 0, length );
            copy.length = length;
            return copy;
        }
    }

    public class RelIdIterator
    {
        private int blockIndex = 1;
        private IdBlock currentBlock = isEmpty() ? EMPTY_BLOCK : blocks.get( 0 );
        private int relativePosition;
        private int absolutePosition;
        private Long nextElement;
        
        public boolean hasNext()
        {
            if ( nextElement != null )
            {
                return true;
            }
            
            while ( true )
            {
                int blockLength = currentBlock.length();
                if ( relativePosition < blockLength )
                {
                    nextElement = currentBlock.get( relativePosition++ );
                    return true;
                }
                else
                {
                    if ( blockIndex < blocks.size() )
                    {
                        goToNextBlock();
                    }
                    else
                    {
                        break;
                    }
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
            long result = nextElement;
            nextElement = null;
            return result;
        }
        
        public void fastForwardTo( int position )
        {
            while ( this.absolutePosition < position )
            {
                goToNextBlock();
            }
            this.absolutePosition = position;
        }

        private void goToNextBlock()
        {
            int leftInBlock = currentBlock.length-relativePosition;
            absolutePosition += leftInBlock;
            currentBlock = blocks.get( ++blockIndex );
            relativePosition = 0;
        }
        
        public int position()
        {
            return absolutePosition;
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
//            int newLength = 0;
//            if ( add != null )
//            {
//                newLength += add.length();
//            }
//            if ( src != null )
//            {
//                newLength += src.length();
//            }
            RelIdArray newArray = new RelIdArray();
            newArray.addAll( src );
            Set<Long> removedSet = remove.asSet();
            evictExcluded( newArray, removedSet );
            if ( add != null )
            {
                for ( RelIdIterator iterator = add.iterator(); iterator.hasNext(); )
                {
                    long value = iterator.next();
                    if ( !removedSet.contains( value ) )
                    {
                        newArray.add( value );
                    }
                }
            }
            return newArray;
        }
    }

    private static void evictExcluded( RelIdArray ids, Set<Long> excluded )
    {
        for ( RelIdIterator iterator = ids.iterator(); iterator.hasNext(); )
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
        Set<Long> set = new HashSet<Long>( length() + 1, 1.0f );
        for ( RelIdIterator iterator = iterator(); iterator.hasNext(); )
        {
            set.add( iterator.next() );
        }
        return set;
    }
}
