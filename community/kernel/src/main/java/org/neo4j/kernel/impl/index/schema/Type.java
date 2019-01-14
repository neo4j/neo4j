/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.index.schema;

import java.util.Comparator;
import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.util.Comparator.comparing;
import static org.neo4j.kernel.impl.index.schema.GenericKey.TRUE;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.HIGH;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.LOW;

/**
 * All functionality for reading, writing, comparing, calculating size etc a specific value type in a native index.
 * This is not an enum mostly because of arrays having a shared subclass with lots of shared functionality applicable to all array types.
 * The type classes are state-less singletons and operate on state in {@link GenericKey} which is passed in as argument to the methods.
 * <p>
 * Looking solely at {@link GenericKey} is has a bunch of state with no specific meaning, but they get meaning when looking at them
 * through a {@link Type}, where e.g. the fields `long0` and `long1` means perhaps string length of a string or the integer value for a number type.
 */
abstract class Type
{
    private static final long MASK_BOOLEAN = 0x1;

    /**
     * Compares {@link Type types} against each other. The ordering adheres to that of {@link ValueGroup}.
     */
    static final Comparator<Type> COMPARATOR = comparing( t -> t.valueGroup );

    /**
     * {@link ValueGroup} for values that this type manages.
     */
    final ValueGroup valueGroup;

    /**
     * An internal id of this type, also persisted into the actual keys in the tree.
     * WARNING: changing ids of existing types will change the index format.
     */
    final byte typeId;

    /**
     * Minimum possible value of this type.
     */
    private final Value minValue;

    /**
     * Maximum possible value of this type.
     */
    private final Value maxValue;

    Type( ValueGroup valueGroup, byte typeId, Value minValue, Value maxValue )
    {
        this.valueGroup = valueGroup;
        this.typeId = typeId;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    /**
     * Size of the key state of this type in the given {@link GenericKey}.
     * @param state the {@link GenericKey} holding the initialized key state.
     * @return size, in bytes of the key state, not counting tree overhead or entity id.
     */
    abstract int valueSize( GenericKey state );

    /**
     * Copies key state from {@code from} to {@code to}.
     * @param to key state to copy into.
     * @param from key state to copy from.
     */
    abstract void copyValue( GenericKey to, GenericKey from );

    /**
     * Calculates minimal splitter between {@code left} and {@code right} and copies that state, potentially a sub-part of that state into {@code into}.
     * @param left left key state to compare.
     * @param right right key state to compare.
     * @param into state which gets initialized with the minimal splitter key state between {@code left} and {@code right}.
     */
    void minimalSplitter( GenericKey left, GenericKey right, GenericKey into )
    {
        // if not a specific implementation then default is to just copy from 'right'
        into.copyFrom( right );
    }

    /**
     * Materializes the key state into an actual {@link Value} object.
     * @param state key state to materialize a {@link Value} from.
     * @return a {@link Value} from the given {@code state}.
     */
    abstract Value asValue( GenericKey state );

    /**
     * Compares {@code left} and {@code right} key state. Follows semantics of {@link Comparator#compare(Object, Object)}.
     * @param left left key state to compare.
     * @param right right key state to compare.
     * @return comparison between the {@code left} and {@code right} key state.
     */
    abstract int compareValue( GenericKey left, GenericKey right );

    /**
     * Serializes key state from {@code state} into the {@code cursor}.
     * @param cursor {@link PageCursor} initialized at correct offset, capable of writing the key state.
     * @param state key state to write to the {@code cursor}.
     */
    abstract void putValue( PageCursor cursor, GenericKey state );

    /**
     * Deserializes key state from {@code cursor} into {@code state}.
     * @param cursor {@link PageCursor} initialized at correct offset to read from.
     * @param size total number of remaining bytes for this key state.
     * @param into {@link GenericKey} to deserialize the key state into.
     * @return whether or not this was a sane read. Returning {@code false} should mean that it was simply a bad read,
     * and that the next read in this shouldRetry loop will get a good read. This will signal that it's not worth it to read any further
     * for this key and that the cursor have been told about this error, via {@link PageCursor#setCursorException(String)}.
     * Otherwise, for a successful read, returns {@code true}.
     */
    abstract boolean readValue( PageCursor cursor, int size, GenericKey into );

    /**
     * Initializes key state to be the lowest possible of this type, i.e. all actual key states of this type are bigger in comparison.
     * @param state key state to initialize as lowest of this type.
     */
    void initializeAsLowest( GenericKey state )
    {
        state.writeValue( minValue, LOW );
    }

    /**
     * Initializes key state to be the highest possible of this type, i.e. all actual key states of this type are smaller in comparison.
     * @param state key state to initialize as highest of this type.
     */
    void initializeAsHighest( GenericKey state )
    {
        state.writeValue( maxValue, HIGH );
    }

    /**
     * Generate a string-representation of the key state of this type, mainly for debugging purposes.
     * @param state the key state containing the state to generate string representation for.
     * @return a string-representation of the key state of this type.
     */
    String toString( GenericKey state )
    {
        // For most types it's a straight-forward Value#toString().
        return asValue( state ).toString();
    }

    static byte[] ensureBigEnough( byte[] array, int targetLength )
    {
        return array == null || array.length < targetLength ? new byte[targetLength] : array;
    }

    static byte[][] ensureBigEnough( byte[][] array, int targetLength )
    {
        return array == null || array.length < targetLength ? new byte[targetLength][] : array;
    }

    static long[] ensureBigEnough( long[] array, int targetLength )
    {
        return array == null || array.length < targetLength ? new long[targetLength] : array;
    }

    static boolean booleanOf( long longValue )
    {
        return (longValue & MASK_BOOLEAN) == TRUE;
    }

    String toDetailedString( GenericKey state )
    {
        StringJoiner joiner = new StringJoiner( ", " );
        joiner.add( toString( state ) );

        // Mutable, meta-state
        joiner.add( "type=" + state.type.getClass().getSimpleName() );
        joiner.add( "inclusion=" + state.inclusion );
        joiner.add( "isArray=" + state.isArray );

        addTypeSpecificDetails( joiner, state );
        return joiner.toString();
    }

    protected abstract void addTypeSpecificDetails( StringJoiner joiner, GenericKey state );
}
