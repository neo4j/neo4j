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

/**
 * Table for storing and retrieving all entries managed by the {@link HopScotchHashingAlgorithm}.
 * A table is responsible for storing and retrieving:
 * <ol>
 *   <li>keys: a primitive long key</li>
 *   <li>hop bits: a concept used by the hop-scotch algorithm to manage hash conflicts, a.k.a. neighbor</li>
 *   <li>values: (OPTIONAL) value associated with each key. Optional in a setting where only the key is
 *       important, e.g. a set</li>
 * </ol>
 *
 * The {@link HopScotchHashingAlgorithm} contains all hashing logic and a {@link Table} is a dump, straight
 * forward data keeper. Optimizations for special keys or key/value combinations can be captured in
 * implementations of this. The only restriction is that keys must be decimal type numbers, f.ex. {@code short},
 * {@code int} or {@code long}. Where the key "carrier" between the algorithm and the table is always going to be
 * {@code long} since it can carry all the others.
 *
 * {@link #version() versioning} is added to the table interface, but implementations can disable versioning
 * by always returning the same and constant number in both {@link #version()} and {@link #version(int)}.
 * Versioning helps iterating over a constant set of entries at the same time as modifying the table.
 */
public interface Table<VALUE> extends AutoCloseable
{
    /**
     * @return {@code H} as defined by the hop-scotch algorithm, i.e. how many entries can share the same
     * table index, i.e. how many conflicts there can be at most for any given table index.
     */
    int h();

    /**
     * @return number of entries this table can hold at most.
     */
    int capacity();

    /**
     * @return number of entries this table holds at the moment.
     */
    int size();

    /**
     * @return a bit mask for doing table length modulo, for example when incrementing or decrementing a table
     * index, so that it wraps around the edges.
     */
    int mask();

    /**
     * @param index the table index to get the key for.
     * @return the key at the given index. If there's no assigned key here then {@link #nullKey()} will be returned.
     */
    long key( int index );

    /**
     * OPTIONAL operation. Returns {@code null} if unsupported.
     * @param index
     * @return the value at the given index. If there's no assigned key here then {@code null} will be returned.
     */
    VALUE value( int index );

    /**
     * Puts (key/value) at the given {@code index}. This index must contractually be free at the point where the
     * {@link HopScotchHashingAlgorithm algorithm} calls this method.
     * @param index the index to put this key/value in.
     * @param key the key to put.
     * @param value the value to put.
     */
    void put( int index, long key, VALUE value );

    /**
     * Puts, actually overwrites, the value at the given {@code index}. This index will contractually be occupied
     * at the point where the {@link HopScotchHashingAlgorithm algorithm} calls this method. This new {@code value}
     * replaces the existing value at this index.
     * @param index the index to put this value in.
     * @param value the value to put at this index.
     * @return the existing value at this index, before this new value was put there.
     */
    VALUE putValue( int index, VALUE value );

    /**
     * Moves the key/value from one index to another, any hop bits stay. This is equivalent to a remove at
     * {@code fromIndex} followed by a put at {@code toIndex}. After this method has been called there will no longer
     * be any key/value assigned at {@code fromIndex}.
     * @param fromIndex the index to move the key/value from.
     * @param toIndex the index to move the key/value to.
     * @return the affected key.
     */
    long move( int fromIndex, int toIndex );

    /**
     * Removes the currently assigned key/value from the given {@code index}.
     * @param index the index to remove key/value from.
     * @return the existing value at this index.
     */
    VALUE remove( int index );

    /**
     * A short summary of the hop bits format: It's a bit map represented as an int, where each set bit represents
     * an offset where there's a neighbor for this index.
     *
     * Here is the two least significant bytes in an example bit map carrying hop bits:
     * <pre>
     * ....[0000,0001][0000,0100]
     * </pre>
     * In the above example, if we assume the index carrying these hop bits is 5, then index 5 has two neighbors:
     * index 8 and index 14. (the least significant bit represents an offset of 1). Maximum number of hop bits
     * is controlled by {@link #h()}.
     *
     * Interesting to note is that hop bits for an index and key/value are not really associated, where a key/value
     * can be moved to a new location, but the hop bits stay behind. The location of Hop bits
     * is tied to table index, whereas the location of key/value is tied to table index AND hop bits.
     *
     * @param index the index to get the hop bits for.
     * @return the hop bits for the given index.
     */
    long hopBits( int index );

    /**
     * Adds one hop bit to the set of hop bits at the given index.
     * @param index the index to add the hop bit at.
     * @param hd h-delta, i.e. which hop bit to set, zero-based.
     * @see #hopBits(int)
     */
    void putHopBit( int index, int hd );

    /**
     * Moves one hop bit {@code delta} steps. If delta is positive it will be moved towards msb,
     * if negative towards lsb.
     * @param index the index to move the hop bit for.
     * @param hd the hop bit to move.
     * @param delta how far (and based on sign) which direction to move it in.
     */
    void moveHopBit( int index, int hd, int delta );

    /**
     * Removes one hop bit from the set of hop bits at the given index.
     * @param index the index to remove the hop bit from.
     * @param hd h-delta, i.e. which hop bit to remove, zero-based.
     * @see #hopBits(int)
     */
    void removeHopBit( int index, int hd );

    /**
     * @return key representing an index that has not been assigned. Why is this a method? Since an implementation
     * could based this value on {@link #h()} for example, which although is final per table instance,
     * changes from table to table.
     */
    long nullKey();

    /**
     * Grows the table to double that of the current size. This method should not populate the table,
     * it's just responsible for growing the structure holding the entries. All entries should have {@link #nullKey()}
     * after this method has been called.
     * The {@link #size()} should be reset here as well since after it has grown then it will be re-populated
     * immediately with the existing data.
     * @return the new table.
     */
    Table<VALUE> grow();

    /**
     * @return {@code true} if there are no entries in this table, otherwise {@code false}.
     */
    boolean isEmpty();

    /**
     * Removes all entries from this table.
     */
    void clear();

    /**
     * @return the version of the table, i.e. the version of the newest entry in the table.
     */
    int version();

    /**
     * @param index the index of the entry to get.
     * @return the version of the entry by the given {@code index} in this table.
     */
    int version( int index );

    /**
     * Free any resources
     */
    @Override
    public void close();
}
