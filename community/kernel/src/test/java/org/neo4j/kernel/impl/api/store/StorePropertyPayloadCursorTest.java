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
package org.neo4j.kernel.impl.api.store;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.store.StorePropertyCursor.payloadValueAsObject;
import static org.neo4j.kernel.impl.api.store.StorePropertyPayloadCursorTest.Param.param;
import static org.neo4j.kernel.impl.api.store.StorePropertyPayloadCursorTest.Param.paramArg;
import static org.neo4j.kernel.impl.api.store.StorePropertyPayloadCursorTest.Params.params;
import static org.neo4j.test.Assert.assertObjectOrArrayEquals;

@RunWith( Enclosed.class )
public class StorePropertyPayloadCursorTest
{
    public static class BasicContract
    {
        @Test
        public void shouldBeOkToClearUnusedCursor()
        {
            // Given
            StorePropertyPayloadCursor cursor = newCursor( "cat-dog" );

            // When
            cursor.clear();

            // Then
            // clear() on an unused cursor works just fine
        }

        @Test
        public void shouldBeOkToClearPartiallyExhaustedCursor()
        {
            // Given
            StorePropertyPayloadCursor cursor = newCursor( 1, 2, 3L );

            assertTrue( cursor.next() );
            assertTrue( cursor.next() );

            // When
            cursor.clear();

            // Then
            // clear() on an used cursor works just fine
        }

        @Test
        public void shouldBeOkToClearExhaustedCursor()
        {
            // Given
            StorePropertyPayloadCursor cursor = newCursor( 1, 2, 3 );

            assertTrue( cursor.next() );
            assertTrue( cursor.next() );
            assertTrue( cursor.next() );

            // When
            cursor.clear();

            // Then
            // clear() on an exhausted cursor works just fine
        }

        @Test
        public void shouldBePossibleToCallClearOnEmptyCursor()
        {
            // Given
            StorePropertyPayloadCursor cursor = newCursor();

            // When
            cursor.clear();

            // Then
            // clear() on an empty cursor works just fine
        }

        @Test
        public void shouldBePossibleToCallNextOnEmptyCursor()
        {
            // Given
            StorePropertyPayloadCursor cursor = newCursor();

            // When
            assertFalse( cursor.next() );

            // Then
            // next() on an empty cursor works just fine
        }

        @Test
        public void shouldUseDynamicStringAndArrayStoresThroughDifferentCursors()
        {
            // Given
            DynamicStringStore dynamicStringStore = newDynamicStoreMock( DynamicStringStore.class );
            DynamicArrayStore dynamicArrayStore = newDynamicStoreMock( DynamicArrayStore.class );

            String string = RandomStringUtils.randomAlphanumeric( 5000 );
            byte[] array = RandomStringUtils.randomAlphanumeric( 10000 ).getBytes();
            StorePropertyPayloadCursor cursor = newCursor( dynamicStringStore, dynamicArrayStore, string, array );

            // When
            assertTrue( cursor.next() );
            assertNotNull( cursor.stringValue() );

            assertTrue( cursor.next() );
            assertNotNull( cursor.arrayValue() );

            assertFalse( cursor.next() );

            // Then
            verify( dynamicStringStore ).newDynamicRecordCursor();
            verify( dynamicArrayStore ).newDynamicRecordCursor();
        }

        @Test
        public void nextMultipleInvocations()
        {
            StorePropertyPayloadCursor cursor = newCursor();

            assertFalse( cursor.next() );
            assertFalse( cursor.next() );
            assertFalse( cursor.next() );
            assertFalse( cursor.next() );
        }

    }

    @RunWith( Parameterized.class )
    public static class SingleValuePayload
    {
        @Parameter( 0 )
        public Param param;

        @Parameterized.Parameters( name = "{0}" )
        public static List<Object[]> parameters()
        {
            return Arrays.asList(
                    param( false, PropertyType.BOOL ),
                    param( true, PropertyType.BOOL ),
                    param( (byte) 24, PropertyType.BYTE ),
                    param( Byte.MIN_VALUE, PropertyType.BYTE ),
                    param( Byte.MAX_VALUE, PropertyType.BYTE ),
                    param( (short) 99, PropertyType.SHORT ),
                    param( Short.MIN_VALUE, PropertyType.SHORT ),
                    param( Short.MAX_VALUE, PropertyType.SHORT ),
                    param( 'c', PropertyType.CHAR ),
                    param( Character.MIN_LOW_SURROGATE, PropertyType.CHAR ),
                    param( Character.MAX_HIGH_SURROGATE, PropertyType.CHAR ),
                    param( 10293, PropertyType.INT ),
                    param( Integer.MIN_VALUE, PropertyType.INT ),
                    param( Integer.MAX_VALUE, PropertyType.INT ),
                    param( (float) 564.29393, PropertyType.FLOAT ),
                    param( Float.MIN_VALUE, PropertyType.FLOAT ),
                    param( Float.MAX_VALUE, PropertyType.FLOAT ),
                    param( 93039173.12848, PropertyType.DOUBLE ),
                    param( Double.MIN_VALUE, PropertyType.DOUBLE ),
                    param( Double.MAX_VALUE, PropertyType.DOUBLE ),
                    param( 484381293L, PropertyType.LONG ),
                    param( Long.MIN_VALUE, PropertyType.LONG ),
                    param( Long.MAX_VALUE, PropertyType.LONG ),
                    param( "short", PropertyType.SHORT_STRING ),
                    param( "alongershortstring", PropertyType.SHORT_STRING ),
                    param( "areallylongshortstringbutstillnotsobig", PropertyType.SHORT_STRING ),
                    param( new boolean[]{true}, PropertyType.SHORT_ARRAY ),
                    param( new byte[]{(byte) 250}, PropertyType.SHORT_ARRAY ),
                    param( new short[]{(short) 12000}, PropertyType.SHORT_ARRAY ),
                    param( new char[]{'T'}, PropertyType.SHORT_ARRAY ),
                    param( new int[]{314}, PropertyType.SHORT_ARRAY ),
                    param( new float[]{(float) 3.14}, PropertyType.SHORT_ARRAY ),
                    param( new double[]{314159.2653}, PropertyType.SHORT_ARRAY ),
                    param( new long[]{1234567890123L}, PropertyType.SHORT_ARRAY )
            );
        }

        @Test
        public void shouldReturnCorrectSingleValue()
        {
            // Given
            StorePropertyPayloadCursor cursor = newCursor( param );

            // When
            boolean next = cursor.next();

            // Then
            assertTrue( next );
            assertEquals( param.type, cursor.type() );
            assertObjectOrArrayEquals( param.value, payloadValueAsObject( cursor ) );
        }
    }

    @RunWith( Parameterized.class )
    public static class MultipleValuePayload
    {
        @Parameter( 0 )
        public Params parameters;

        @Parameterized.Parameters( name = "{0}" )
        public static List<Object[]> parameters()
        {
            return Arrays.asList(
                    params(
                            paramArg( false, PropertyType.BOOL ),
                            paramArg( true, PropertyType.BOOL )
                    ),
                    params(
                            paramArg( (byte) 24, PropertyType.BYTE ),
                            paramArg( Byte.MIN_VALUE, PropertyType.BYTE )
                    ),
                    params(
                            paramArg( (short) 99, PropertyType.SHORT ),
                            paramArg( Short.MAX_VALUE, PropertyType.SHORT )
                    ),
                    params(
                            paramArg( 'c', PropertyType.CHAR ),
                            paramArg( Character.MAX_HIGH_SURROGATE, PropertyType.CHAR )
                    ),
                    params(
                            paramArg( 10293, PropertyType.INT ),
                            paramArg( Integer.MIN_VALUE, PropertyType.INT )
                    ),
                    params(
                            paramArg( (float) 564.29393, PropertyType.FLOAT ),
                            paramArg( Float.MAX_VALUE, PropertyType.FLOAT )
                    ),
                    params(
                            paramArg( Double.MAX_VALUE, PropertyType.DOUBLE ),
                            paramArg( Double.MIN_VALUE, PropertyType.DOUBLE )
                    ),
                    params(
                            paramArg( Long.MIN_VALUE, PropertyType.LONG ),
                            paramArg( Long.MAX_VALUE, PropertyType.LONG )
                    ),
                    params(
                            paramArg( new boolean[]{true}, PropertyType.SHORT_ARRAY ),
                            paramArg( new byte[]{(byte) 250}, PropertyType.SHORT_ARRAY )
                    ),
                    params(
                            paramArg( new short[]{(short) 12000}, PropertyType.SHORT_ARRAY ),
                            paramArg( new short[]{Short.MIN_VALUE}, PropertyType.SHORT_ARRAY )
                    ),
                    params(
                            paramArg( new char[]{'T'}, PropertyType.SHORT_ARRAY ),
                            paramArg( new int[]{314}, PropertyType.SHORT_ARRAY )
                    ),
                    params(
                            paramArg( new float[]{(float) 3.14}, PropertyType.SHORT_ARRAY ),
                            paramArg( new long[]{1234567890123L}, PropertyType.SHORT_ARRAY )
                    ),
                    params(
                            paramArg( new double[]{Double.MIN_VALUE}, PropertyType.SHORT_ARRAY ),
                            paramArg( new long[]{Long.MAX_VALUE}, PropertyType.SHORT_ARRAY )
                    ),
                    params(
                            paramArg( new long[]{1234567890123L}, PropertyType.SHORT_ARRAY ),
                            paramArg( new long[]{Long.MIN_VALUE}, PropertyType.SHORT_ARRAY )
                    ),
                    params(
                            paramArg( new long[]{1234567890123L}, PropertyType.SHORT_ARRAY ),
                            paramArg( new long[]{Long.MIN_VALUE}, PropertyType.SHORT_ARRAY )
                    ),
                    params(
                            paramArg( false, PropertyType.BOOL ),
                            paramArg( true, PropertyType.BOOL ),
                            paramArg( false, PropertyType.BOOL ),
                            paramArg( true, PropertyType.BOOL )
                    ),
                    params(
                            paramArg( (byte) 24, PropertyType.BYTE ),
                            paramArg( true, PropertyType.BOOL ),
                            paramArg( (short) 99, PropertyType.SHORT )
                    ),
                    params(
                            paramArg( Byte.MIN_VALUE, PropertyType.BYTE ),
                            paramArg( Byte.MAX_VALUE, PropertyType.BYTE ),
                            paramArg( (short) 99, PropertyType.SHORT ),
                            paramArg( true, PropertyType.BOOL )
                    ),
                    params(
                            paramArg( (short) 99, PropertyType.SHORT ),
                            paramArg( (byte) 1, PropertyType.BYTE ),
                            paramArg( Short.MIN_VALUE, PropertyType.SHORT ),
                            paramArg( Short.MAX_VALUE, PropertyType.SHORT )
                    ),
                    params(
                            paramArg( Short.MAX_VALUE, PropertyType.SHORT ),
                            paramArg( 5L, PropertyType.LONG ),
                            paramArg( 6L, PropertyType.LONG )
                    ),
                    params(
                            paramArg( 'c', PropertyType.CHAR ),
                            paramArg( 'h', PropertyType.CHAR ),
                            paramArg( 'a', PropertyType.CHAR ),
                            paramArg( 'r', PropertyType.CHAR )
                    ),
                    params(
                            paramArg( 10293, PropertyType.INT ),
                            paramArg( 'r', PropertyType.CHAR ),
                            paramArg( 3.14, PropertyType.DOUBLE )
                    ),
                    params(
                            paramArg( Integer.MIN_VALUE, PropertyType.INT ),
                            paramArg( Integer.MAX_VALUE, PropertyType.INT ),
                            paramArg( Integer.MAX_VALUE, PropertyType.INT ),
                            paramArg( Integer.MAX_VALUE, PropertyType.INT )
                    ),
                    params(
                            paramArg( Float.MIN_VALUE, PropertyType.FLOAT ),
                            paramArg( (float) 256.256, PropertyType.FLOAT )
                    ),
                    params(
                            paramArg( Double.MIN_VALUE + 1, PropertyType.DOUBLE ),
                            paramArg( Double.MAX_VALUE - 1, PropertyType.DOUBLE )
                    ),
                    params(
                            paramArg( Double.MIN_VALUE, PropertyType.DOUBLE ),
                            paramArg( Short.MAX_VALUE, PropertyType.SHORT ),
                            paramArg( Byte.MAX_VALUE, PropertyType.BYTE )
                    ),
                    params(
                            paramArg( 484381293L, PropertyType.LONG ),
                            paramArg( 'c', PropertyType.CHAR ),
                            paramArg( 1, PropertyType.INT ),
                            paramArg( true, PropertyType.BOOL )
                    ),
                    params(
                            paramArg( 's', PropertyType.CHAR ),
                            paramArg( 'o', PropertyType.CHAR ),
                            paramArg( "rt", PropertyType.SHORT_STRING ),
                            paramArg( true, PropertyType.BOOL )
                    ),
                    params(
                            paramArg( "abc", PropertyType.SHORT_STRING ),
                            paramArg( 11L, PropertyType.LONG )
                    ),
                    params(
                            paramArg( new boolean[]{true}, PropertyType.SHORT_ARRAY ),
                            paramArg( new boolean[]{true, false, false}, PropertyType.SHORT_ARRAY ),
                            paramArg( new byte[]{(byte) 1024}, PropertyType.SHORT_ARRAY )
                    ),

                    params(
                            paramArg( new byte[]{(byte) 250, (byte) 251, (byte) 252}, PropertyType.SHORT_ARRAY ),
                            paramArg( new char[]{'C', 'T'}, PropertyType.SHORT_ARRAY ),
                            paramArg( true, PropertyType.BOOL )
                    ),
                    params(
                            paramArg( new long[]{1234567890123L, Long.MAX_VALUE}, PropertyType.SHORT_ARRAY ),
                            paramArg( (byte) 42, PropertyType.BYTE )
                    )
            );
        }

        @Test
        public void shouldReturnCorrectValues()
        {
            // Given
            StorePropertyPayloadCursor cursor = newCursor( parameters );

            for ( Param param : parameters )
            {
                // When
                boolean next = cursor.next();

                // Then
                assertTrue( next );
                assertEquals( param.type, cursor.type() );
                assertObjectOrArrayEquals( param.value, payloadValueAsObject( cursor ) );
            }
        }
    }

    private static StorePropertyPayloadCursor newCursor( Params input )
    {
        return newCursor( input.params );
    }

    private static StorePropertyPayloadCursor newCursor( Param... params )
    {
        Object[] values = new Object[params.length];
        for ( int i = 0; i < params.length; i++ )
        {
            values[i] = params[i].value;
        }
        return newCursor( values );
    }

    private static StorePropertyPayloadCursor newCursor( Object... values )
    {
        DynamicStringStore dynamicStringStore = mock( DynamicStringStore.class );
        DynamicArrayStore dynamicArrayStore = mock( DynamicArrayStore.class );

        return newCursor( dynamicStringStore, dynamicArrayStore, values );
    }

    private static StorePropertyPayloadCursor newCursor( DynamicStringStore dynamicStringStore,
            DynamicArrayStore dynamicArrayStore, Object... values )
    {
        StorePropertyPayloadCursor cursor = new StorePropertyPayloadCursor( dynamicStringStore, dynamicArrayStore );

        PageCursor pageCursor = newPageCursor( values );
        cursor.init( pageCursor );

        return cursor;
    }

    private static PageCursor newPageCursor( Object... values )
    {
        RecordAllocator stringAllocator = new RecordAllocator();
        RecordAllocator arrayAllocator = new RecordAllocator();

        ByteBuffer page = ByteBuffer.allocateDirect( StorePropertyPayloadCursor.MAX_NUMBER_OF_PAYLOAD_LONG_ARRAY * 8 );
        for ( int i = 0; i < values.length; i++ )
        {
            Object value = values[i];

            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue( block, i, value, stringAllocator, arrayAllocator );
            long[] valueBlocks = block.getValueBlocks();
            for ( long valueBlock : valueBlocks )
            {
                page.putLong( valueBlock );
            }
        }
        while ( page.remaining() > 0 )
        {
            page.put( (byte) 0 );
        }

        return new StubPageCursor( 1, page );
    }

    private static <S extends AbstractDynamicStore> S newDynamicStoreMock( Class<S> clazz )
    {
        AbstractDynamicStore.DynamicRecordCursor recordCursor = mock( AbstractDynamicStore.DynamicRecordCursor.class );
        when( recordCursor.next() ).thenReturn( true ).thenReturn( false );
        DynamicRecord dynamicRecord = new DynamicRecord( 42 );
        dynamicRecord.setData( new byte[]{1, 1, 1, 1, 1} );
        when( recordCursor.get() ).thenReturn( dynamicRecord );

        S store = mock( clazz );
        when( store.newDynamicRecordCursor() ).thenReturn( mock( AbstractDynamicStore.DynamicRecordCursor.class ) );
        when( store.getRecordsCursor( anyLong(), any( AbstractDynamicStore.DynamicRecordCursor.class ) ) )
                .thenReturn( recordCursor );

        return store;
    }

    static class Param
    {
        final Object value;
        final PropertyType type;

        Param( Object value, PropertyType type )
        {
            this.value = value;
            this.type = type;
        }

        static Object[] param( Object value, PropertyType type )
        {
            return new Object[]{paramArg( value, type )};
        }

        static Param paramArg( Object value, PropertyType type )
        {
            return new Param( value, type );
        }

        @Override
        public String toString()
        {
            return "{type=" + type + ", value=" + Strings.prettyPrint( value ) + "}";
        }
    }

    static class Params implements Iterable<Param>
    {
        final Param[] params;

        Params( Param[] params )
        {
            this.params = params;
        }

        static Object[] params( Param... input )
        {
            return new Object[]{new Params( input )};
        }

        @Override
        public Iterator<Param> iterator()
        {
            return IteratorUtil.iterator( params );
        }

        @Override
        public String toString()
        {
            return "{params=" + Arrays.toString( params ) + "}";
        }
    }

    private static class RecordAllocator implements DynamicRecordAllocator
    {
        long id;

        @Override
        public int dataSize()
        {
            return 120;
        }

        @Override
        public DynamicRecord nextUsedRecordOrNew( Iterator<DynamicRecord> recordsToUseFirst )
        {
            DynamicRecord record = new DynamicRecord( id++ );
            record.setCreated();
            record.setInUse( true );
            return record;
        }
    }
}
