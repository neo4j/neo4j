/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StandaloneDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.kernel.impl.api.store.StorePropertyPayloadCursorTest.Param.paramArg;
import static org.neo4j.kernel.impl.api.store.StorePropertyPayloadCursorTest.Params.params;
import static org.neo4j.kernel.impl.store.PropertyType.ARRAY;
import static org.neo4j.kernel.impl.store.PropertyType.BOOL;
import static org.neo4j.kernel.impl.store.PropertyType.BYTE;
import static org.neo4j.kernel.impl.store.PropertyType.CHAR;
import static org.neo4j.kernel.impl.store.PropertyType.DOUBLE;
import static org.neo4j.kernel.impl.store.PropertyType.FLOAT;
import static org.neo4j.kernel.impl.store.PropertyType.INT;
import static org.neo4j.kernel.impl.store.PropertyType.LONG;
import static org.neo4j.kernel.impl.store.PropertyType.SHORT;
import static org.neo4j.kernel.impl.store.PropertyType.SHORT_ARRAY;
import static org.neo4j.kernel.impl.store.PropertyType.SHORT_STRING;
import static org.neo4j.kernel.impl.store.PropertyType.STRING;
import static org.neo4j.test.assertion.Assert.assertObjectOrArrayEquals;

@RunWith( Enclosed.class )
public class StorePropertyPayloadCursorTest
{
    public static class BasicContract
    {
        @Test
        public void nextShouldAlwaysReturnFalseWhenNotInitialized()
        {
            @SuppressWarnings( "unchecked" )
            StorePropertyPayloadCursor cursor =
                    new StorePropertyPayloadCursor( mock( DynamicStringStore.class ), mock( DynamicArrayStore.class ) );

            assertFalse( cursor.next() );

            // Should still be true the Nth time
            assertFalse( cursor.next() );
        }

        @Test
        public void nextShouldAlwaysReturnFalseWhenClosed()
        {
            StorePropertyPayloadCursor cursor = allProperties( params( paramArg( 42, "cat-dog", STRING ) ) );

            assertTrue( cursor.next() );

            cursor.close();

            // Should still be true the Nth time
            assertFalse( cursor.next() );
            assertFalse( cursor.next() );
        }

        @Test
        public void shouldBeOkToCloseAnUnusedCursor()
        {
            // Given
            StorePropertyPayloadCursor cursor = allProperties( params( paramArg( 42, "cat-dog", STRING ) ) );

            // When
            cursor.close();

            // Then
            // close() on an unused cursor works just fine
        }

        @Test
        public void shouldBeOkToClosePartiallyExhaustedCursor()
        {
            // Given
            StorePropertyPayloadCursor cursor =
                    allProperties( params( paramArg( 42, 1, INT ), paramArg( 55, 2, INT ),
                            paramArg( 73, 3L, LONG ) ) );

            assertTrue( cursor.next() );
            assertTrue( cursor.next() );

            // When
            cursor.close();

            // Then
            // close() on an used cursor works just fine
        }

        @Test
        public void shouldBeOkToCloseExhaustedCursor()
        {
            // Given
            StorePropertyPayloadCursor cursor =
                    allProperties( params(paramArg( 42, 1, INT ), paramArg( 55, 2, INT ),
                            paramArg( 73, 3L, INT ) ) );

            assertTrue( cursor.next() );
            assertTrue( cursor.next() );
            assertTrue( cursor.next() );

            // When
            cursor.close();

            // Then
            // close() on an exhausted cursor works just fine
        }

        @Test
        public void shouldBePossibleToCallCloseOnEmptyCursor()
        {
            // Given
            StorePropertyPayloadCursor cursor = allProperties( params() );

            // When
            cursor.close();

            // Then
            // close() on an empty cursor works just fine
        }

        @Test
        public void shouldBePossibleToCallNextOnEmptyCursor()
        {
            // Given
            StorePropertyPayloadCursor cursor = allProperties( params() );

            // When
            assertFalse( cursor.next() );

            // Then
            // next() on an empty cursor works just fine
        }

        @Test
        public void shouldUseDynamicStringAndArrayStoresThroughDifferentCursors() throws Throwable
        {
            // Given
            DynamicStringStore dynamicStringStore = newDynamicStoreMock( DynamicStringStore.class );
            DynamicArrayStore dynamicArrayStore = newDynamicStoreMock( DynamicArrayStore.class );

            Param[] params = {paramArg( 42, RandomStringUtils.randomAlphanumeric( 5000 ), STRING ),
                    paramArg( 55, RandomStringUtils.randomAlphanumeric( 10000 ).getBytes(), ARRAY )};
            StorePropertyPayloadCursor cursor =
                    createCursor( ALWAYS_TRUE_INT, dynamicStringStore, dynamicArrayStore, params );

            // When
            assertTrue( cursor.next() );
            assertNotNull( cursor.value() );

            assertTrue( cursor.next() );
            assertNotNull( cursor.value() );

            assertFalse( cursor.next() );

            // Then
            verify( dynamicStringStore ).newPageCursor();
            verify( dynamicArrayStore ).newPageCursor();
        }

        @Test
        public void nextMultipleInvocations()
        {
            StorePropertyPayloadCursor cursor = allProperties( params() );

            assertFalse( cursor.next() );
            assertFalse( cursor.next() );
            assertFalse( cursor.next() );
            assertFalse( cursor.next() );
        }

        @Test
        public void shouldClosePageCursorWhenDisposed()
        {
            // given
            DynamicStringStore stringStore = mock( DynamicStringStore.class );
            PageCursor stringCursor = mock( PageCursor.class );
            when( stringStore.newPageCursor() ).thenReturn( stringCursor );
            DynamicArrayStore arrayStore = mock( DynamicArrayStore.class );
            PageCursor arrayCursor = mock( PageCursor.class );
            when( arrayStore.newPageCursor() ).thenReturn( arrayCursor );
            StorePropertyPayloadCursor cursor = createCursor( ALWAYS_TRUE_INT, stringStore, arrayStore, new Param[0] );
            cursor.close();

            // when
            cursor.dispose();

            // then
            verify( stringCursor ).close();
            verify( arrayCursor ).close();
        }
    }

    @RunWith( Parameterized.class )
    public static class SingleAndMultipleValuePayload
    {
        @Parameter( 0 )
        public Params parameters;

        @Parameterized.Parameters( name = "{0}" )
        public static List<Object[]> parameters()
        {
            return Arrays.asList( new Object[]{params( paramArg( 42, false, BOOL ) )},
                    new Object[]{params( paramArg( 42, true, BOOL ) )},
                    new Object[]{params( paramArg( 42, (byte) 24, BYTE ) )},
                    new Object[]{params( paramArg( 42, Byte.MIN_VALUE, BYTE ) )},
                    new Object[]{params( paramArg( 42, Byte.MAX_VALUE, BYTE ) )},
                    new Object[]{params( paramArg( 42, (short) 99, SHORT ) )},
                    new Object[]{params( paramArg( 42, Short.MIN_VALUE, SHORT ) )},
                    new Object[]{params( paramArg( 42, Short.MAX_VALUE, SHORT ) )},
                    new Object[]{params( paramArg( 42, 'c', CHAR ) )},
                    new Object[]{params( paramArg( 42, Character.MIN_LOW_SURROGATE, CHAR ) )},
                    new Object[]{params( paramArg( 42, Character.MAX_HIGH_SURROGATE, CHAR ) )},
                    new Object[]{params( paramArg( 42, 10293, INT ) )},
                    new Object[]{params( paramArg( 42, Integer.MIN_VALUE, INT ) )},
                    new Object[]{params( paramArg( 42, Integer.MAX_VALUE, INT ) )},
                    new Object[]{params( paramArg( 42, (float) 564.29393, FLOAT ) )},
                    new Object[]{params( paramArg( 42, Float.MIN_VALUE, FLOAT ) )},
                    new Object[]{params( paramArg( 42, Float.MAX_VALUE, FLOAT ) )},
                    new Object[]{params( paramArg( 42, 93039173.12848, DOUBLE ) )},
                    new Object[]{params( paramArg( 42, Double.MIN_VALUE, DOUBLE ) )},
                    new Object[]{params( paramArg( 42, Double.MAX_VALUE, DOUBLE ) )},
                    new Object[]{params( paramArg( 42, 484381293L, LONG ) )},
                    new Object[]{params( paramArg( 42, Long.MIN_VALUE, LONG ) )},
                    new Object[]{params( paramArg( 42, Long.MAX_VALUE, LONG ) )},
                    new Object[]{params( paramArg( 42, "short", SHORT_STRING ) )},
                    new Object[]{params( paramArg( 42, "alongershortstring", SHORT_STRING ) )},
                    new Object[]{params( paramArg( 42, "areallylongshortstringbutstillnotsobig", SHORT_STRING ) )},
                    new Object[]{params( paramArg( 42, new boolean[]{true}, SHORT_ARRAY ) )},
                    new Object[]{params( paramArg( 42, new byte[]{(byte) 250}, SHORT_ARRAY ) )},
                    new Object[]{params( paramArg( 42, new short[]{(short) 12000}, SHORT_ARRAY ) )},
                    new Object[]{params( paramArg( 42, new char[]{'T'}, SHORT_ARRAY ) )},
                    new Object[]{params( paramArg( 42, new int[]{314}, SHORT_ARRAY ) )},
                    new Object[]{params( paramArg( 42, new float[]{(float) 3.14}, SHORT_ARRAY ) )},
                    new Object[]{params( paramArg( 42, new double[]{314159.2653}, SHORT_ARRAY ) )},
                    new Object[]{params( paramArg( 42, new long[]{1234567890123L}, SHORT_ARRAY ) )},
                    new Object[]{params( paramArg( 42, false, BOOL ), paramArg( 55, true, BOOL ) )},
                    new Object[]{params( paramArg( 42, (byte) 24, BYTE ), paramArg( 55, Byte.MIN_VALUE, BYTE ) )},
                    new Object[]{params( paramArg( 42, (short) 99, SHORT ), paramArg( 55, Short.MAX_VALUE, SHORT ) )},
                    new Object[]{params(
                            paramArg( 42, 'c', CHAR ),
                            paramArg( 55, Character.MAX_HIGH_SURROGATE, CHAR)
                    )},
                    new Object[]{params(
                            paramArg( 42, 10293, INT ),
                            paramArg( 55, Integer.MIN_VALUE, INT )
                    )},
                    new Object[]{params(
                            paramArg( 42, (float) 564.29393, FLOAT ),
                            paramArg( 55, Float.MAX_VALUE, FLOAT )
                    )},
                    new Object[]{params(
                            paramArg( 42, Double.MAX_VALUE, DOUBLE ),
                            paramArg( 55, Double.MIN_VALUE, DOUBLE )
                    )},
                    new Object[]{params(
                            paramArg( 42, Long.MIN_VALUE, LONG ),
                            paramArg( 55, Long.MAX_VALUE, LONG )
                    )},
                    new Object[]{params(
                            paramArg( 42, new boolean[]{true}, SHORT_ARRAY ),
                            paramArg( 55, new byte[]{(byte) 250}, SHORT_ARRAY )
                    )},
                    new Object[]{params(
                            paramArg( 42, new short[]{(short) 12000}, SHORT_ARRAY ),
                            paramArg( 55, new short[]{Short.MIN_VALUE}, SHORT_ARRAY )
                    )},
                    new Object[]{params(
                            paramArg( 42, new char[]{'T'}, SHORT_ARRAY ),
                            paramArg( 55, new int[]{314}, SHORT_ARRAY )
                    )},
                    new Object[]{params(
                            paramArg( 42, new float[]{(float) 3.14}, SHORT_ARRAY ),
                            paramArg( 55, new long[]{1234567890123L}, SHORT_ARRAY )
                    )},
                    new Object[]{params(
                            paramArg( 42, new double[]{Double.MIN_VALUE}, SHORT_ARRAY ),
                            paramArg( 55, new long[]{Long.MAX_VALUE}, SHORT_ARRAY )
                    )},
                    new Object[]{params(
                            paramArg( 42, new long[]{1234567890123L}, SHORT_ARRAY ),
                            paramArg( 55, new long[]{Long.MIN_VALUE}, SHORT_ARRAY )
                    )},
                    new Object[]{params(
                            paramArg( 42, new long[]{1234567890123L}, SHORT_ARRAY ),
                            paramArg( 55, new long[]{Long.MIN_VALUE}, SHORT_ARRAY )
                    )},
                    new Object[]{params(
                            paramArg( 42, false, BOOL ),
                            paramArg( 55, true, BOOL ),
                            paramArg( 73, false, BOOL ),
                            paramArg( 99, true, BOOL )
                    )},
                    new Object[]{params(
                            paramArg( 42, (byte) 24, BYTE ),
                            paramArg( 55, true, BOOL ),
                            paramArg( 73, (short) 99, SHORT )
                    )},
                    new Object[]{params(
                            paramArg( 42, Byte.MIN_VALUE, BYTE ),
                            paramArg( 55, Byte.MAX_VALUE, BYTE ),
                            paramArg( 73, (short) 99, SHORT ),
                            paramArg( 99, true, BOOL )
                    )},
                    new Object[]{params(
                            paramArg( 42, (short) 99, SHORT ),
                            paramArg( 55, (byte) 1, BYTE ),
                            paramArg( 73, Short.MIN_VALUE, SHORT ),
                            paramArg( 99, Short.MAX_VALUE, SHORT )
                    )},
                    new Object[]{params(
                            paramArg( 42, Short.MAX_VALUE, SHORT ),
                            paramArg( 55, 5L, LONG ),
                            paramArg( 73, 6L, LONG )
                    )},
                    new Object[]{params(
                            paramArg( 42, 'c', CHAR ),
                            paramArg( 55, 'h', CHAR ),
                            paramArg( 73, 'a', CHAR ),
                            paramArg( 99, 'r', CHAR )
                    )},
                    new Object[]{params(
                            paramArg( 42, 10293, INT ),
                            paramArg( 55, 'r', CHAR ),
                            paramArg( 73, 3.14, DOUBLE )
                    )},
                    new Object[]{params(
                            paramArg( 42, Integer.MIN_VALUE, INT ),
                            paramArg( 55, Integer.MAX_VALUE, INT ),
                            paramArg( 73, Integer.MAX_VALUE, INT ),
                            paramArg( 99, Integer.MAX_VALUE, INT )
                    )},
                    new Object[]{params(
                            paramArg( 42, Float.MIN_VALUE, FLOAT ),
                            paramArg( 55, (float) 256.256, FLOAT )
                    )},
                    new Object[]{params(
                            paramArg( 42, Double.MIN_VALUE + 1, DOUBLE ),
                            paramArg( 55, Double.MAX_VALUE - 1, DOUBLE )
                    )},
                    new Object[]{params(
                            paramArg( 42, Double.MIN_VALUE, DOUBLE ),
                            paramArg( 55, Short.MAX_VALUE, SHORT ),
                            paramArg( 73, Byte.MAX_VALUE, BYTE )
                    )},
                    new Object[]{params(
                            paramArg( 42, 484381293L, LONG ),
                            paramArg( 55, 'c', CHAR ),
                            paramArg( 73, 1, INT ),
                            paramArg( 99, true, BOOL )
                    )},
                    new Object[]{params(
                            paramArg( 42, 's', CHAR ),
                            paramArg( 55, 'o', CHAR ),
                            paramArg( 73, "rt", SHORT_STRING ),
                            paramArg( 99, true, BOOL )
                    )},
                    new Object[]{params(
                            paramArg( 42, "abc", SHORT_STRING ),
                            paramArg( 55, 11L, LONG )
                    )},
                    new Object[]{params(
                            paramArg( 42, new boolean[]{true}, SHORT_ARRAY ),
                            paramArg( 55, new boolean[]{true, false, false}, SHORT_ARRAY ),
                            paramArg( 73, new byte[]{(byte) 1024}, SHORT_ARRAY )
                    )},
                    new Object[]{params(
                            paramArg( 42, new byte[]{(byte) 250, (byte) 251, (byte) 252}, SHORT_ARRAY ),
                            paramArg( 55, new char[]{'C', 'T'}, SHORT_ARRAY ),
                            paramArg( 73, true, BOOL )
                    )},
                    new Object[]{params(
                            paramArg( 42, new long[]{1234567890123L, Long.MAX_VALUE}, SHORT_ARRAY ),
                            paramArg( 55, (byte) 42, BYTE )
                    )}
            );
        }

        @Test
        public void shouldReturnCorrectValues()
        {
            // Given
            StorePropertyPayloadCursor cursor = allProperties( parameters );

            for ( Param param : parameters )
            {
                // When
                boolean next = cursor.next();

                // Then
                assertNextAndSameValue( next, param, cursor.type(), cursor.value() );
            }
            // and then
            assertFalse( cursor.next() );
        }

        @Test
        public void shouldReturnCorrectSingleValueOrNone()
        {
            // Given
            int[] keyIds = {42, 55, 73, 99};
            for ( int keyId : keyIds )
            {
                StorePropertyPayloadCursor cursor = singleProperty( keyId, parameters );

                for ( Param param : Iterables.filter( ( param ) -> param.keyId == keyId, parameters ) )
                {
                    // When
                    boolean next = cursor.next();

                    // Then
                    assertNextAndSameValue( next, param, cursor.type(), cursor.value() );

                }
                // and then
                assertFalse( cursor.next() );
            }
        }

        @Test
        public void shouldReturnNothingForNonExistentPropertyKeyId()
        {
            // Given
            StorePropertyPayloadCursor cursor = singleProperty( 21, parameters );
            // When/Then
            assertFalse( cursor.next() );
        }
    }

    private static void assertNextAndSameValue( boolean next, Param param, PropertyType type, Object value )
    {
        assertTrue( next );
        assertEquals( param.type, type );
        assertObjectOrArrayEquals( param.value, value );
    }

    private static StorePropertyPayloadCursor allProperties( Params input )
    {
        DynamicStringStore dynamicStringStore = mock( DynamicStringStore.class );
        DynamicArrayStore dynamicArrayStore = mock( DynamicArrayStore.class );
        return createCursor( ALWAYS_TRUE_INT, dynamicStringStore, dynamicArrayStore, input.params );
    }

    private static StorePropertyPayloadCursor singleProperty( int keyId, Params input )
    {
        DynamicStringStore dynamicStringStore = mock( DynamicStringStore.class );
        DynamicArrayStore dynamicArrayStore = mock( DynamicArrayStore.class );
        return createCursor( k -> k == keyId, dynamicStringStore, dynamicArrayStore, input.params );
    }

    private static StorePropertyPayloadCursor createCursor( IntPredicate allPropertyKeyIds,
            DynamicStringStore dynamicStringStore, DynamicArrayStore dynamicArrayStore, Param[] params )
    {
        StorePropertyPayloadCursor cursor = new StorePropertyPayloadCursor( dynamicStringStore, dynamicArrayStore );
        long[] blocks = asBlocks( params );
        cursor.init( allPropertyKeyIds, blocks, blocks.length );
        return cursor;
    }

    private static long[] asBlocks( Param... params )
    {
        DynamicRecordAllocator stringAllocator = new StandaloneDynamicRecordAllocator();
        DynamicRecordAllocator arrayAllocator = new StandaloneDynamicRecordAllocator();
        long[] blocks = new long[PropertyType.getPayloadSizeLongs()];
        int cursor = 0;
        for ( int i = 0; i < params.length; i++ )
        {
            Param param = params[i];

            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue( block, param.keyId, param.value, stringAllocator, arrayAllocator );
            long[] valueBlocks = block.getValueBlocks();
            System.arraycopy( valueBlocks, 0, blocks, cursor, valueBlocks.length );
            cursor += valueBlocks.length;
        }
        return blocks;
    }

    @SuppressWarnings( "unchecked" )
    private static <S extends AbstractDynamicStore> S newDynamicStoreMock( Class<S> clazz ) throws IOException
    {
        S store = mock( clazz );
        doAnswer( invocationOnMock ->
        {
            DynamicRecord record = (DynamicRecord) invocationOnMock.getArguments()[1];
            record.setId( 42 );
            record.setData( new byte[]{1, 1, 1, 1, 1} );
            return null;
        } ).when( store ).readIntoRecord( anyLong(), any( DynamicRecord.class ), any( RecordLoad.class ),
                any( PageCursor.class ) );

        return store;
    }

    static class Param
    {
        final Object value;
        final PropertyType type;
        final int keyId;

        private Param( Object value, PropertyType type, int keyId )
        {
            this.value = value;
            this.type = type;
            this.keyId = keyId;
        }

        static Param paramArg( int keyId, Object value, PropertyType type )
        {
            return new Param( value, type, keyId );
        }

        @Override
        public String toString()
        {
            return "Param{keyId=" + keyId + ", type=" + type + ", value=" + Strings.prettyPrint( value ) + '}';
        }
    }

    static class Params implements Iterable<Param>
    {
        final Param[] params;

        private Params( Param[] params )
        {
            this.params = params;
        }

        static Params params( Param... input )
        {
            return new Params( input );
        }

        @Override
        public Iterator<Param> iterator()
        {
            return Iterators.iterator( params );
        }

        @Override
        public String toString()
        {
            return Arrays.toString( params );
        }
    }
}
