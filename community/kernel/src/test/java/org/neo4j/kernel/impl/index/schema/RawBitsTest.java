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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class RawBitsTest
{
    @Parameterized.Parameter()
    public String name;

    @Parameterized.Parameter( 1 )
    public NumberLayout layout;

    @Parameterized.Parameters( name = "{0}" )
    public static List<Object[]> layouts()
    {
        return asList(
                new Object[]{"Unique",
                        new NumberLayoutUnique()
                },
                new Object[]{"NonUnique",
                        new NumberLayoutNonUnique()
                }
        );
    }

    final List<Object> objects = Arrays.asList(
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MIN_VALUE + 1,
            Integer.MIN_VALUE,
            Short.MIN_VALUE,
            Byte.MIN_VALUE,
            0,
            Double.MIN_VALUE,
            Double.MIN_NORMAL,
            Float.MIN_VALUE,
            Float.MIN_NORMAL,
            1L,
            1.1d,
            1.2f,
            Math.E,
            Math.PI,
            (byte) 10,
            (short) 20,
            Byte.MAX_VALUE,
            Short.MAX_VALUE,
            Integer.MAX_VALUE,
            33554432,
            33554432F,
            33554433,
            33554433F,
            33554434,
            33554434F,
            9007199254740991L,
            9007199254740991D,
            9007199254740992L,
            9007199254740992D,
            9007199254740993L,
            9007199254740993D,
            9007199254740994L,
            9007199254740994D,
            Long.MAX_VALUE,
            Float.MAX_VALUE,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY,
            Double.NaN,
            Math.nextDown( Math.E ),
            Math.nextUp( Math.E ),
            Math.nextDown( Math.PI ),
            Math.nextUp( Math.PI )
    );

    @Test
    public void mustSortInSameOrderAsValueComparator()
    {
        // given
        List<Value> values = asValueObjects( objects );
        List<NumberSchemaKey> schemaNumberKeys = asSchemaNumberKeys( values );
        Collections.shuffle( values );
        Collections.shuffle( schemaNumberKeys );

        // when
        values.sort( Values.COMPARATOR );
        schemaNumberKeys.sort( layout );
        List<Value> actual = asValues( schemaNumberKeys );

        // then
        assertSameOrder( actual, values );
    }

    @Test
    public void shouldCompareAllValuesToAllOtherValuesLikeValueComparator()
    {
        // given
        List<Value> values = asValueObjects( objects );
        List<NumberSchemaKey> schemaNumberKeys = asSchemaNumberKeys( values );
        values.sort( Values.COMPARATOR );

        // when
        for ( NumberSchemaKey numberKey : schemaNumberKeys )
        {
            List<NumberSchemaKey> withoutThisOne = new ArrayList<>( schemaNumberKeys );
            assertTrue( withoutThisOne.remove( numberKey ) );
            withoutThisOne = unmodifiableList( withoutThisOne );
            for ( int i = 0; i < withoutThisOne.size(); i++ )
            {
                List<NumberSchemaKey> withThisOneInWrongPlace = new ArrayList<>( withoutThisOne );
                withThisOneInWrongPlace.add( i, numberKey );
                withThisOneInWrongPlace.sort( layout );
                List<Value> actual = asValues( withThisOneInWrongPlace );

                // then
                assertSameOrder( actual, values );
            }
        }
    }

    @Test
    public void shouldHaveSameCompareResultsAsValueCompare()
    {
        // given
        List<Value> values = asValueObjects( objects );
        List<NumberSchemaKey> schemaNumberKeys = asSchemaNumberKeys( values );

        // when
        for ( int i = 0; i < values.size(); i++ )
        {
            Value value1 = values.get( i );
            NumberSchemaKey schemaNumberKey1 = schemaNumberKeys.get( i );
            for ( int j = 0; j < values.size(); j++ )
            {
                // then
                Value value2 = values.get( j );
                NumberSchemaKey schemaNumberKey2 = schemaNumberKeys.get( j );
                assertEquals( Values.COMPARATOR.compare( value1, value2 ),
                        layout.compare( schemaNumberKey1, schemaNumberKey2 ) );
                assertEquals( Values.COMPARATOR.compare( value2, value1 ),
                        layout.compare( schemaNumberKey2, schemaNumberKey1 ) );
            }
        }
    }

    private List<Value> asValues( List<NumberSchemaKey> schemaNumberKeys )
    {
        return schemaNumberKeys.stream()
                .map( k -> RawBits.asNumberValue( k.rawValueBits, k.type ) )
                .collect( Collectors.toList() );
    }

    private void assertSameOrder( List<Value> actual, List<Value> values )
    {
        assertEquals( actual.size(), values.size() );
        for ( int i = 0; i < actual.size(); i++ )
        {
            Number actualAsNumber = (Number) actual.get( i ).asObject();
            Number valueAsNumber = (Number) values.get( i ).asObject();
            //noinspection StatementWithEmptyBody
            if ( Double.isNaN( actualAsNumber.doubleValue() ) && Double.isNaN( valueAsNumber.doubleValue() ) )
            {
                // Don't compare equals because NaN does not equal itself
            }
            else
            {
                assertEquals( actual.get( i ), values.get( i ) );
            }
        }
    }

    private List<Value> asValueObjects( List<Object> objects )
    {
        List<Value> values = new ArrayList<>();
        for ( Object object : objects )
        {
            values.add( Values.of( object ) );
        }
        return values;
    }

    private List<NumberSchemaKey> asSchemaNumberKeys( List<Value> values )
    {
        List<NumberSchemaKey> schemaNumberKeys = new ArrayList<>();
        for ( Value value : values )
        {
            NumberSchemaKey key = new NumberSchemaKey();
            key.from( 0, value );
            schemaNumberKeys.add( key );
        }
        return schemaNumberKeys;
    }
}
