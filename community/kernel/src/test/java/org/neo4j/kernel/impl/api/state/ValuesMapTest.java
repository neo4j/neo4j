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
package org.neo4j.kernel.impl.api.state;

import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.function.Function0;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.block.procedure.primitive.LongObjectProcedure;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.values.storable.Values.intValue;

@ExtendWith( RandomExtension.class )
class ValuesMapTest
{
    @Inject
    private RandomRule rnd;

    private final ValuesMap map = newMap();

    @Test
    void putGet()
    {
        map.put( 0, intValue( 10 ) );
        map.put( 1, intValue( 11 ) );
        map.put( 2, intValue( 12 ) );

        assertEquals( intValue( 10 ), map.get( 0 ) );
        assertEquals( intValue( 11 ), map.get( 1 ) );
        assertEquals( intValue( 12 ), map.get( 2 ) );
        // default empty value
        assertNull( map.get( 3 ) );
    }

    @Test
    void putAll()
    {
        map.putAll( LongObjectHashMap.newWithKeysValues( 0, intValue( 10 ), 1, intValue( 11 ), 2, intValue( 12 ) ) );
        assertEquals( 3, map.size() );
        assertEquals( intValue( 10 ), map.get( 0 ) );
        assertEquals( intValue( 11 ), map.get( 1 ) );
        assertEquals( intValue( 12 ), map.get( 2 ) );
    }

    @Test
    void getIfAbsent()
    {
        assertEquals( intValue( -1 ), map.getIfAbsent( 0, () -> intValue( -1 ) ) );
        assertEquals( intValue( -1 ), map.getIfAbsent( 1, () -> intValue( -1 ) ) );
        assertEquals( intValue( -1 ), map.getIfAbsent( 2, () -> intValue( -1 ) ) );
        assertEquals( intValue( -1 ), map.getIfAbsent( 3, () -> intValue( -1 ) ) );

        map.putAll( LongObjectHashMap.newWithKeysValues( 0, intValue( 10 ), 1, intValue( 11 ), 2, intValue( 12 ) ) );

        assertEquals( intValue( 10 ), map.getIfAbsent( 0, () -> intValue( -1 ) ) );
        assertEquals( intValue( 11 ), map.getIfAbsent( 1, () -> intValue( -1 ) ) );
        assertEquals( intValue( 12 ), map.getIfAbsent( 2, () -> intValue( -1 ) ) );
        assertEquals( intValue( -1 ), map.getIfAbsent( 3, () -> intValue( -1 ) ) );
    }

    @Test
    void getIfAbsentPut()
    {
        assertEquals( intValue( 10 ), map.getIfAbsentPut( 0, intValue( 10 ) ) );
        assertEquals( intValue( 10 ), map.getIfAbsentPut( 0, intValue( 100 ) ) );
        assertEquals( intValue( 11 ), map.getIfAbsentPut( 1, intValue( 11 ) ) );
        assertEquals( intValue( 11 ), map.getIfAbsentPut( 1, intValue( 110 ) ) );
        assertEquals( intValue( 12 ), map.getIfAbsentPut( 2, intValue( 12 ) ) );
        assertEquals( intValue( 12 ), map.getIfAbsentPut( 2, intValue( 120 ) ) );
    }

    @Test
    void getIfAbsentPut_Supplier()
    {
        final Function0<Value> supplier = mock( Function0.class );
        doReturn( intValue( 10 ), intValue( 11 ), intValue( 12 ) ).when( supplier ).value();

        assertEquals( intValue( 10 ), map.getIfAbsentPut( 0, supplier ) );
        assertEquals( intValue( 11 ), map.getIfAbsentPut( 1, supplier ) );
        assertEquals( intValue( 12 ), map.getIfAbsentPut( 2, supplier ) );
        verify( supplier, times( 3 ) ).value();

        assertEquals( intValue( 10 ), map.getIfAbsentPut( 0, supplier ) );
        assertEquals( intValue( 11 ), map.getIfAbsentPut( 1, supplier ) );
        assertEquals( intValue( 12 ), map.getIfAbsentPut( 2, supplier ) );
        verifyNoMoreInteractions( supplier );
    }

    @Test
    void getIfAbsentPutWithKey()
    {
        @SuppressWarnings( "Convert2Lambda" )
        final LongToObjectFunction<Value> function = spy( new LongToObjectFunction<Value>()
        {
            @Override
            public Value valueOf( long x )
            {
                return intValue( 10 + (int) x );
            }
        } );

        assertEquals( intValue( 10 ), map.getIfAbsentPutWithKey( 0, function ) );
        assertEquals( intValue( 10 ), map.getIfAbsentPutWithKey( 0, function ) );
        assertEquals( intValue( 11 ), map.getIfAbsentPutWithKey( 1, function ) );
        assertEquals( intValue( 11 ), map.getIfAbsentPutWithKey( 1, function ) );
        assertEquals( intValue( 12 ), map.getIfAbsentPutWithKey( 2, function ) );
        assertEquals( intValue( 12 ), map.getIfAbsentPutWithKey( 2, function ) );

        verify( function ).valueOf( eq( 0L ) );
        verify( function ).valueOf( eq( 1L ) );
        verify( function ).valueOf( eq( 2L ) );
        verifyNoMoreInteractions( function );
    }

    @Test
    void getIfAbsentPutWith()
    {
        @SuppressWarnings( {"Convert2Lambda", "Anonymous2MethodRef"} )
        final Function<String, Value> function = spy( new Function<String, Value>()
        {
            @Override
            public Value valueOf( String s )
            {
                return intValue( Integer.valueOf( s ) );
            }
        } );

        assertEquals( intValue( 10 ), map.getIfAbsentPutWith( 0, function, "10" ) );
        assertEquals( intValue( 10 ), map.getIfAbsentPutWith( 0, function, "10" ) );
        assertEquals( intValue( 11 ), map.getIfAbsentPutWith( 1, function, "11" ) );
        assertEquals( intValue( 11 ), map.getIfAbsentPutWith( 1, function, "11" ) );
        assertEquals( intValue( 12 ), map.getIfAbsentPutWith( 2, function, "12" ) );
        assertEquals( intValue( 12 ), map.getIfAbsentPutWith( 2, function, "12" ) );

        verify( function ).valueOf( eq( "10" ) );
        verify( function ).valueOf( eq( "11" ) );
        verify( function ).valueOf( eq( "12" ) );
        verifyNoMoreInteractions( function );
    }

    @Test
    void putOverwrite()
    {
        map.putAll( LongObjectHashMap.newWithKeysValues( 0, intValue( 10 ), 1, intValue( 11 ), 2, intValue( 12 ) ) );

        assertEquals( intValue( 10 ), map.get( 0 ) );
        assertEquals( intValue( 11 ), map.get( 1 ) );
        assertEquals( intValue( 12 ), map.get( 2 ) );

        map.putAll( LongObjectHashMap.newWithKeysValues( 0, intValue( 20 ), 1, intValue( 21 ), 2, intValue( 22 ) ) );

        assertEquals( intValue( 20 ), map.get( 0 ) );
        assertEquals( intValue( 21 ), map.get( 1 ) );
        assertEquals( intValue( 22 ), map.get( 2 ) );
    }

    @Test
    void size()
    {
        assertEquals( 0, map.size() );
        map.put( 0, intValue( 10 ) );
        assertEquals( 1, map.size() );
        map.put( 1, intValue( 11 ) );
        assertEquals( 2, map.size() );
        map.put( 2, intValue( 12 ) );
        assertEquals( 3, map.size() );
        map.put( 0, intValue( 20 ) );
        map.put( 1, intValue( 20 ) );
        map.put( 2, intValue( 20 ) );
        assertEquals( 3, map.size() );
        map.remove( 0 );
        assertEquals( 2, map.size() );
        map.remove( 1 );
        assertEquals( 1, map.size() );
        map.remove( 2 );
        assertEquals( 0, map.size() );
    }

    @Test
    void containsKey()
    {
        assertFalse( map.containsKey( 0 ) );
        assertFalse( map.containsKey( 1 ) );
        assertFalse( map.containsKey( 2 ) );

        map.put( 0, intValue( 10 ) );
        assertTrue( map.containsKey( 0 ) );

        map.put( 1, intValue( 11 ) );
        assertTrue( map.containsKey( 1 ) );

        map.put( 2, intValue( 12 ) );
        assertTrue( map.containsKey( 2 ) );
    }

    @Test
    void forEachKey()
    {
        final LongProcedure consumer = mock( LongProcedure.class );
        map.putAll( LongObjectHashMap.newWithKeysValues( 0, intValue( 10 ), 1, intValue( 11 ), 2, intValue( 12 ) ) );

        map.forEachKey( consumer );

        verify( consumer ).value( eq( 0L ) );
        verify( consumer ).value( eq( 1L ) );
        verify( consumer ).value( eq( 2L ) );
        verifyNoMoreInteractions( consumer );
    }

    @Test
    void forEachValue()
    {
        final Procedure<Value> consumer = mock( Procedure.class );
        map.putAll( LongObjectHashMap.newWithKeysValues( 0, intValue( 10 ), 1, intValue( 11 ), 2, intValue( 12 ) ) );

        map.forEachValue( consumer );

        verify( consumer ).value( eq( intValue( 10 ) ) );
        verify( consumer ).value( eq( intValue( 11 ) ) );
        verify( consumer ).value( eq( intValue( 12 ) ) );
        verifyNoMoreInteractions( consumer );
    }

    @Test
    void forEachKeyValue()
    {
        final LongObjectProcedure<Value> consumer = mock( LongObjectProcedure.class );
        map.putAll( LongObjectHashMap.newWithKeysValues( 0, intValue( 10 ), 1, intValue( 11 ), 2, intValue( 12 ) ) );

        map.forEachKeyValue( consumer );

        verify( consumer ).value( eq( 0L ), eq( intValue( 10 ) ) );
        verify( consumer ).value( eq( 1L ), eq( intValue( 11 ) ) );
        verify( consumer ).value( eq( 2L ), eq( intValue( 12 ) ) );
        verifyNoMoreInteractions( consumer );
    }

    @Test
    void clear()
    {
        map.clear();
        assertEquals( 0, map.size() );

        map.putAll( LongObjectHashMap.newWithKeysValues( 0, intValue( 10 ), 1, intValue( 11 ), 2, intValue( 12 ) ) );

        assertEquals( 3, map.size() );

        map.clear();
        assertEquals( 0, map.size() );

        map.clear();
        assertEquals( 0, map.size() );
    }

    @Test
    void randomizedWithSharedValuesContainer()
    {
        final int MAPS = 13;
        final int COUNT = 10000 + rnd.nextInt( 1000 );

        final AppendOnlyValuesContainer valuesContainer = new AppendOnlyValuesContainer( new TestMemoryAllocator() );

        final List<ValuesMap> actualMaps = new ArrayList<>();
        final List<MutableLongObjectMap<Value>> expectedMaps = new ArrayList<>();

        for ( int i = 0; i < MAPS; i++ )
        {
            actualMaps.add( newMap( valuesContainer ) );
            expectedMaps.add( new LongObjectHashMap<>() );
        }

        for ( int i = 0; i < MAPS; i++ )
        {
            put( COUNT, actualMaps.get( i ), expectedMaps.get( i ) );
        }

        for ( int i = 0; i < MAPS; i++ )
        {
            remove( COUNT, actualMaps.get( i ), expectedMaps.get( i ) );
        }

        for ( int i = 0; i < MAPS; i++ )
        {
            final MutableLongObjectMap<Value> expected = expectedMaps.get( i );
            final ValuesMap actual = actualMaps.get( i );
            expected.forEachKeyValue( ( k, v ) -> assertEquals( v, actual.get( k ) ) );
        }
    }

    private void remove( int count, ValuesMap actualMap, MutableLongObjectMap<Value> expectedMap )
    {
        for ( int i = 0; i < count / 2; i++ )
        {
            final long key = rnd.nextLong( count );
            final Value value = rnd.randomValues().nextValue();
            actualMap.put( key, value );
            expectedMap.put( key, value );
        }
    }

    private void put( int count, ValuesMap actualMap, MutableLongObjectMap<Value> expectedMap )
    {
        for ( int i = 0; i < count * 2; i++ )
        {
            final long key = rnd.nextLong( count );
            final Value value = rnd.randomValues().nextValue();
            actualMap.put( key, value );
            expectedMap.put( key, value );
        }
    }

    private static ValuesMap newMap()
    {
        return newMap( new AppendOnlyValuesContainer( new TestMemoryAllocator() ) );
    }

    private static ValuesMap newMap( AppendOnlyValuesContainer valuesContainer )
    {
        return new ValuesMap( new LongLongHashMap(), valuesContainer );
    }
}
