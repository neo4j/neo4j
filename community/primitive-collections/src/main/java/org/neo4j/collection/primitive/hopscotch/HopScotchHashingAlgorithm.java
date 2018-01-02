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
package org.neo4j.collection.primitive.hopscotch;

import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Long.numberOfTrailingZeros;

/**
 * <p>
 * An implementation of the hop-scotch algorithm, see http://en.wikipedia.org/wiki/Hopscotch_hashing.
 * It's a static set of methods that implements the essence of the algorithm, where storing and retrieving data is
 * abstracted into the {@link Table} interface. Also things like {@link Monitor monitoring} and choice of
 * {@link HashFunction} gets passed in.
 * </p>
 *
 * <p>
 * About hop scotching and hop bits: Each index in the table has hop bits which is a list of which nearby
 * indexes contain entries with a key hashing to the same index as itself - its neighborhood.
 * A neighbor can at most be {@code H} indexes away. This is the collision resolution method of choice
 * for the hop scotch algorithm. Getting and putting entries at an index first checks the index at hand.
 * If occupied by another entry its neighbors, dictated by the hop bits, are checked.
 *
 * When putting an entry and the index and all its neighbors are occupied, the hop scotching begins, where a free
 * index further away is picked and iteratively moved closer and closer until it's within the neighborhood
 * of the intended index. The entry is then placed at that newly "freed" index.
 *
 * Removing an entry will put some effort into do reverse hop scotching as well, i.e. moving a neighbor into
 * the newly removed index and iteratively also move neighbors of the moved neighbor, and so forth.
 *
 * This behavior has the benefit of keeping entries hashing to the same index very close together,
 * and so will take more advantage of CPU caches and pre-fetching in general, especially for lookups.
 * </p>
 *
 * <p>
 * Why are these methods (like {@link #put(Table, Monitor, HashFunction, long, Object, ResizeMonitor)},
 * {@link #get(Table, Monitor, HashFunction, long)} a.s.o. static? To reduce garbage and also reduce overhead of each
 * set or map object making use of hop-scotch hashing where they won't need to have a reference to an algorithm
 * object, merely use its static methods. Also, all essential state is managed by {@link Table}.
 * </p>
 */
public class HopScotchHashingAlgorithm
{
    /**
     * Default number of hop bits per index, i.e. size of neighborhood.
     */
    public static final int DEFAULT_H = 32;

    public static <VALUE> VALUE get( Table<VALUE> table, Monitor monitor, HashFunction hashFunction, long key )
    {
        int tableMask = table.mask();
        int index = indexOf( hashFunction, key, tableMask );
        long existingKey = table.key( index );
        if ( existingKey == key )
        {   // Bulls eye
            return table.value( index );
        }

        // Look in its neighborhood
        long hopBits = table.hopBits( index );
        while ( hopBits > 0 )
        {
            int hopIndex = nextIndex( index, numberOfTrailingZeros( hopBits )+1, tableMask );
            if ( table.key( hopIndex ) == key )
            {   // There it is
                return table.value( hopIndex );
            }
            hopBits &= hopBits-1;
        }

        return null;
    }

    public static <VALUE> VALUE remove( Table<VALUE> table, Monitor monitor, HashFunction hashFunction, long key )
    {
        int tableMask = table.mask();
        int index = indexOf( hashFunction, key, tableMask );
        int freedIndex = -1;
        VALUE result = null;
        if ( table.key( index ) == key )
        {   // Bulls eye
            freedIndex = index;
            result = table.remove( index );
        }

        // Look in its neighborhood
        long hopBits = table.hopBits( index );
        while ( hopBits > 0 )
        {
            int hd = numberOfTrailingZeros( hopBits );
            int hopIndex = nextIndex( index, hd+1, tableMask );
            if ( table.key( hopIndex ) == key )
            {   // there it is
                freedIndex = hopIndex;
                result = table.remove( hopIndex );
                table.removeHopBit( index, hd );
            }
            hopBits &= hopBits-1;
        }

        // reversed hop-scotching, i.e. pull in the most distant neighbor, iteratively as long as the
        // pulled index has neighbors of its own
        while ( freedIndex != -1 )
        {
            long freedHopBits = table.hopBits( freedIndex );
            if ( freedHopBits > 0 )
            {   // It's got a neighbor, go ahead and move it here
                int hd = 63-numberOfLeadingZeros( freedHopBits );
                int candidateIndex = nextIndex( freedIndex, hd+1, tableMask );
                // move key/value
                long candidateKey = table.move( candidateIndex, freedIndex );
                // remove that hop bit, since that one is no longer a neighbor, it's "the one" at the index
                table.removeHopBit( freedIndex, hd );
                assert monitor.pulledToFreeIndex( index, table.hopBits( freedIndex ), candidateKey,
                        candidateIndex, freedIndex );
                freedIndex = candidateIndex;
            }
            else
            {
                freedIndex = -1;
            }
        }

        return result;
    }

    public static <VALUE> VALUE put( Table<VALUE> table, Monitor monitor, HashFunction hashFunction,
            long key, VALUE value, ResizeMonitor<VALUE> resizeMonitor )
    {
        long nullKey = table.nullKey();
        assert key != nullKey;
        int tableMask = table.mask();
        int index = indexOf( hashFunction, key, tableMask );
        long keyAtIndex = table.key( index );
        if ( keyAtIndex == nullKey )
        {   // this index is free, just place it there
            table.put( index, key, value );
            assert monitor.placedAtFreeAndIntendedIndex( key, index );
            return null;
        }
        else if ( keyAtIndex == key )
        {   // this index is occupied, but actually with the same key
            return table.putValue( index, value );
        }
        else
        {   // look at the neighbors of this entry to see if any is the requested key
            long hopBits = table.hopBits( index );
            while ( hopBits > 0 )
            {
                int hopIndex = nextIndex( index, numberOfTrailingZeros( hopBits )+1, tableMask );
                if ( table.key( hopIndex ) == key )
                {
                    return table.putValue( hopIndex, value );
                }
                hopBits &= hopBits-1;
            }
        }

        // this key does not exist in this set. put it there using hop-scotching
        if ( hopScotchPut( table, monitor, hashFunction, key, value, index, tableMask, nullKey ) )
        {   // we managed to wiggle our way to a free spot and put it there
            return null;
        }

        // we couldn't add this value, even in the H-1 neighborhood, so grow table...
        growTable( table, monitor, hashFunction, resizeMonitor );
        Table<VALUE> resizedTable = resizeMonitor.getLastTable();

        // ...and try again
        return put( resizedTable, monitor, hashFunction, key, value, resizeMonitor );
    }

    private static <VALUE> boolean hopScotchPut( Table<VALUE> table, Monitor monitor, HashFunction hashFunction,
            long key, VALUE value, int index, int tableMask, long nullKey )
    {
        int freeIndex = nextIndex( index, 1, tableMask );
        int h = table.h();
        int totalHd = 0; // h delta, i.e. distance from first neighbor to current tentative index, the first neighbor has hd=0
        boolean foundFreeSpot = false;

        // linear probe for finding a free slot in ASC index direction
        while ( freeIndex != index ) // one round is enough, albeit far, but at the same time very unlikely
        {
            if ( table.key( freeIndex ) == nullKey )
            {   // free slot found
                foundFreeSpot = true;
                break;
            }

            // move on to the next index in the search for a free slot
            freeIndex = nextIndex( freeIndex, 1, tableMask );
            totalHd++;
        }

        if ( !foundFreeSpot )
        {
            return false;
        }

        while ( totalHd >= h )
        {   // grab a closer index and see which of its neighbors is OK to move further away,
            // so that there will be a free space to place the new value. I.e. move the free space closer
            // and some close neighbors a bit further away (although never outside its neighborhood)
            int neighborIndex = nextIndex( freeIndex, -(h-1), tableMask ); // hopscotch hashing says to try h-1 entries closer

            boolean swapped = false;
            for ( int d = 0; d < (h >> 1) && !swapped; d++ )
            {   // examine hop information (i.e. is there's someone in the neighborhood here to swap with 'hopIndex'?)
                final long neighborHopBitsFixed = table.hopBits( neighborIndex );
                long neighborHopBits = neighborHopBitsFixed;
                while ( neighborHopBits > 0 && !swapped )
                {
                    int hd = numberOfTrailingZeros( neighborHopBits );
                    if ( hd+d >= h-1 )
                    {   // that would be too far
                        break;
                    }
                    neighborHopBits &= neighborHopBits-1;
                    int candidateIndex = nextIndex( neighborIndex, hd+1, tableMask );

                    // OK, here's a neighbor, let's examine it's neighbors (candidates to move)
                    //  - move the candidate entry (incl. updating its hop bits) to the free index
                    int distance = (freeIndex-candidateIndex)&tableMask;
                    long candidateKey = table.move( candidateIndex, freeIndex );
                    //  - update the neighbor entry with the move of the candidate entry
                    table.moveHopBit( neighborIndex, hd, distance );
                    assert monitor.pushedToFreeIndex( index, neighborHopBitsFixed, table.hopBits( neighborIndex ),
                            neighborIndex, candidateKey, candidateIndex, freeIndex );
                    freeIndex = candidateIndex;
                    swapped = true;
                    totalHd -= distance;
                }
                if ( !swapped )
                {
                    neighborIndex = nextIndex( neighborIndex, 1, tableMask );
                }
            }

            if ( !swapped )
            {   // we could not make any room to swap, tell that to the outside world
                return false;
            }
        }

        // OK, now we're within distance to just place it there. Do it
        table.put( freeIndex, key, value );
        // and update the hop bits of "index"
        table.putHopBit( index, totalHd );
        assert monitor.placedAtFreedIndex( index, table.hopBits( index ), key, freeIndex );

        return true;
    }

    private static int nextIndex( int index, int delta, int mask )
    {
        return (index+delta)&mask;
    }

    private static int indexOf( HashFunction hashFunction, long key, int tableMask )
    {
        return hashFunction.hash( key ) & tableMask;
    }

    private static <VALUE> void growTable( Table<VALUE> oldTable, Monitor monitor,
            HashFunction hashFunction, ResizeMonitor<VALUE> resizeMonitor )
    {
        assert monitor.tableGrowing( oldTable.capacity(), oldTable.size() );
        Table<VALUE> newTable = oldTable.grow();
        // Install the new table before populating it with the old data, in case we find it needs to grow even more
        // while we are populating it. If that happens, we want to end up with the table installed by the final grow.
        resizeMonitor.tableGrew( newTable );
        long nullKey = oldTable.nullKey();

        // place all entries in the new table
        int capacity = oldTable.capacity();
        for ( int i = 0; i < capacity; i++ )
        {
            long key = oldTable.key( i );
            if ( key != nullKey )
            {
                // Always use the table from the resize monitor, because any put can cause a grow.
                Table<VALUE> table = resizeMonitor.getLastTable();
                VALUE putResult = put( table, monitor, hashFunction, key, oldTable.value( i ), resizeMonitor );
                if ( putResult != null )
                {
                    // If we somehow fail to populate the new table, reinstall the old one.
                    resizeMonitor.tableGrew( oldTable );
                    newTable.close();
                    throw new IllegalStateException( "Couldn't add " + key + " when growing table" );
                }
            }
        }
        assert monitor.tableGrew( oldTable.capacity(), newTable.capacity(), newTable.size() );
        oldTable.close();
    }

    /**
     * Monitor for what how a {@link HopScotchHashingAlgorithm} changes the items in a {@link Table}.
     */
    public interface Monitor
    {
        boolean tableGrowing( int fromCapacity, int currentSize );

        boolean tableGrew( int fromCapacity, int toCapacity, int currentSize );

        boolean placedAtFreeAndIntendedIndex( long key, int index );

        boolean pushedToFreeIndex( int intendedIndex, long oldHopBits, long newHopBits, int neighborIndex,
                long key, int fromIndex, int toIndex );

        boolean placedAtFreedIndex( int intendedIndex, long newHopBits, long key, int actualIndex );

        boolean pulledToFreeIndex( int intendedIndex, long newHopBits, long key, int fromIndex, int toIndex );

        public abstract static class Adapter implements Monitor
        {
            @Override
            public boolean placedAtFreedIndex( int intendedIndex, long newHopBits, long key, int actualIndex )
            {
                return true;
            }

            @Override
            public boolean placedAtFreeAndIntendedIndex( long key, int index )
            {
                return true;
            }

            @Override
            public boolean pushedToFreeIndex( int intendedIndex, long oldHopBits, long newHopBits,
                    int neighborIndex, long key, int fromIndex, int toIndex )
            {
                return true;
            }

            @Override
            public boolean pulledToFreeIndex( int intendedIndex, long newHopBits, long key,
                    int fromIndex, int toIndex )
            {
                return true;
            }

            @Override
            public boolean tableGrowing( int fromCapacity, int currentSize )
            {
                return true;
            }

            @Override
            public boolean tableGrew( int fromCapacity, int toCapacity, int currentSize )
            {
                return true;
            }
        }
    }

    public static final Monitor NO_MONITOR = new Monitor.Adapter() { /*No additional logic*/ };

    public interface HashFunction
    {
        int hash( long value );
    }

    /**
     * Same hash function as that used by the standard library hash collections. It generates a hash by splitting the
     * input value into segments, and then re-distributing those segments, so the end result is effectively a striped
     * and then jumbled version of the input data. For randomly distributed keys, this has a good chance at generating
     * an even hash distribution over the full hash space.
     *
     * It performs exceptionally poorly for sequences of numbers, as the sequence increments all end up in the same
     * stripe, generating hash values that will end up in the same buckets in collections.
     */
    public static final HashFunction JUL_HASHING = new HashFunction()
    {
        @Override
        public int hash( long value )
        {
            int h = (int) ((value >>> 32) ^ value);
            h ^= (h >>> 20) ^ (h >>> 12);
            return h ^ (h >>> 7) ^ (h >>> 4);
        }
    };

    /**
     * The default hash function is based on a pseudo-random number generator, which uses the input value as a seed
     * to the generator. This is very fast, and performs well for most input data. However, it is not guaranteed to
     * generate a superb distribution, only a "decent" one.
     */
    public static final HashFunction DEFAULT_HASHING = new HashFunction()
    {
        @Override
        public int hash( long value )
        {
            value ^= (value << 21);
            value ^= (value >>> 35);
            value ^= (value << 4);

            return (int) ((value >>> 32) ^ value);
        }
    };

    public interface ResizeMonitor<VALUE>
    {
        void tableGrew( Table<VALUE> newTable );

        Table<VALUE> getLastTable();
    }
}
