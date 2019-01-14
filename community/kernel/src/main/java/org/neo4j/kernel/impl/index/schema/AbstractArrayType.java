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

import java.time.LocalDate;
import java.util.StringJoiner;
import java.util.function.IntFunction;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

import static java.lang.Integer.min;
import static org.neo4j.kernel.impl.index.schema.GenericKey.BIGGEST_REASONABLE_ARRAY_LENGTH;
import static org.neo4j.kernel.impl.index.schema.GenericKey.SIZE_ARRAY_LENGTH;
import static org.neo4j.kernel.impl.index.schema.GenericKey.setCursorException;
import static org.neo4j.kernel.impl.index.schema.GenericKey.toNonNegativeShortExact;

/**
 * Common ancestor of all array-types. Many of the methods are implemented by doing array looping and delegating array item operations
 * to the non-array versions of the specific array type.
 * @param <T> type of raw array items for this array type, e.g. {@link LocalDate} for {@link DateArrayType}.
 */
abstract class AbstractArrayType<T> extends Type
{
    private final ArrayElementComparator arrayElementComparator;
    private final ArrayElementValueFactory<T> valueFactory;
    final ArrayElementWriter arrayElementWriter;
    private final ArrayElementReader arrayElementReader;
    private final IntFunction<T[]> arrayCreator;
    private final ValueWriter.ArrayType arrayType;

    AbstractArrayType( ValueGroup valueGroup, byte typeId,
            ArrayElementComparator arrayElementComparator,
            ArrayElementValueFactory<T> valueFactory,
            ArrayElementWriter arrayElementWriter,
            ArrayElementReader arrayElementReader,
            IntFunction<T[]> arrayCreator,
            ValueWriter.ArrayType arrayType )
    {
        super( valueGroup, typeId,
                // null intentionally as we're overriding how min/max gets applied for all array types
                null, null );
        this.arrayElementComparator = arrayElementComparator;
        this.valueFactory = valueFactory;
        this.arrayElementWriter = arrayElementWriter;
        this.arrayElementReader = arrayElementReader;
        this.arrayCreator = arrayCreator;
        this.arrayType = arrayType;
    }

    @Override
    final void copyValue( GenericKey to, GenericKey from )
    {
        copyValue( to, from, from.arrayLength );
    }

    abstract void copyValue( GenericKey to, GenericKey from, int arrayLength );

    abstract void initializeArray( GenericKey key, int length, ValueWriter.ArrayType arrayType );

    @Override
    void minimalSplitter( GenericKey left, GenericKey right, GenericKey into )
    {
        int lastEqualIndex = -1;
        if ( left.type == right.type )
        {
            int maxLength = min( left.arrayLength, right.arrayLength );
            for ( int index = 0; index < maxLength; index++ )
            {
                if ( arrayElementComparator.compare( left, right, index ) != 0 )
                {
                    break;
                }
                lastEqualIndex++;
            }
        }
        // Convert from last equal index to first index to differ +1
        // Convert from index to length +1
        // Total +2
        int length = Math.min( right.arrayLength, lastEqualIndex + 2 );
        copyValue( into, right, length );
        into.arrayLength = length;
    }

    @Override
    int compareValue( GenericKey left, GenericKey right )
    {
        if ( left.isHighestArray || right.isHighestArray )
        {
            return Boolean.compare( left.isHighestArray, right.isHighestArray );
        }

        int index = 0;
        int compare = 0;
        int length = min( left.arrayLength, right.arrayLength );

        for ( ; compare == 0 && index < length; index++ )
        {
            compare = arrayElementComparator.compare( left, right, index );
        }

        return compare == 0 ? Integer.compare( left.arrayLength, right.arrayLength ) : compare;
    }

    @Override
    Value asValue( GenericKey state )
    {
        T[] array = arrayCreator.apply( state.arrayLength );
        for ( int i = 0; i < state.arrayLength; i++ )
        {
            array[i] = valueFactory.from( state, i );
        }
        return Values.of( array );
    }

    @Override
    void putValue( PageCursor cursor, GenericKey state )
    {
        putArray( cursor, state, arrayElementWriter );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKey into )
    {
        return readArray( cursor, arrayType, arrayElementReader, into );
    }

    /**
     * In the array case there's nothing lower than a zero-length array, so simply make sure that the key state is initialized
     * with state reflecting that. No specific value required.
     * @param state key state to initialize as lowest of this type.
     */
    @Override
    void initializeAsLowest( GenericKey state )
    {
        state.initializeArrayMeta( 0 );
        initializeArray( state, 0, arrayType );
    }

    @Override
    void initializeAsHighest( GenericKey state )
    {
        state.initializeArrayMeta( 0 );
        initializeArray( state, 0, arrayType );
        state.isHighestArray = true;
    }

    int arrayKeySize( GenericKey key, int elementSize )
    {
        return SIZE_ARRAY_LENGTH + key.arrayLength * elementSize;
    }

    static void putArrayHeader( PageCursor cursor, short arrayLength )
    {
        cursor.putShort( arrayLength );
    }

    static void putArrayItems( PageCursor cursor, GenericKey key, ArrayElementWriter itemWriter )
    {
        for ( int i = 0; i < key.arrayLength; i++ )
        {
            itemWriter.write( cursor, key, i );
        }
    }

    static void putArray( PageCursor cursor, GenericKey key, ArrayElementWriter writer )
    {
        putArrayHeader( cursor, toNonNegativeShortExact( key.arrayLength ) );
        putArrayItems( cursor, key, writer );
    }

    static boolean readArray( PageCursor cursor, ValueWriter.ArrayType type, ArrayElementReader reader, GenericKey into )
    {
        if ( !setArrayLengthWhenReading( into, cursor, cursor.getShort() ) )
        {
            return false;
        }
        into.beginArray( into.arrayLength, type );
        for ( int i = 0; i < into.arrayLength; i++ )
        {
            if ( !reader.readFrom( cursor, into ) )
            {
                return false;
            }
        }
        into.endArray();
        return true;
    }

    static boolean setArrayLengthWhenReading( GenericKey state, PageCursor cursor, short arrayLength )
    {
        state.arrayLength = arrayLength;
        if ( state.arrayLength < 0 || state.arrayLength > BIGGEST_REASONABLE_ARRAY_LENGTH )
        {
            setCursorException( cursor, "non-valid array length, " + state.arrayLength );
            state.arrayLength = 0;
            return false;
        }
        return true;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "isHighestArray=" + state.isHighestArray );
        joiner.add( "arrayLength=" + state.arrayLength );
        joiner.add( "currentArrayOffset=" + state.currentArrayOffset );
    }

    @FunctionalInterface
    interface ArrayElementComparator
    {
        int compare( GenericKey o1, GenericKey o2, int i );
    }

    @FunctionalInterface
    interface ArrayElementReader
    {
        boolean readFrom( PageCursor cursor, GenericKey into );
    }

    @FunctionalInterface
    interface ArrayElementWriter
    {
        void write( PageCursor cursor, GenericKey key, int i );
    }

    @FunctionalInterface
    interface ArrayElementValueFactory<T>
    {
        T from( GenericKey key, int i );
    }
}
