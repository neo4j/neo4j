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
package org.neo4j.helpers;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

public class ValueUtilsTest
{
    @Test
    public void shouldHandleCollection()
    {
        // Given
        Collection<Integer> collection = Arrays.asList( 1, 2, 3 );

        // When

        AnyValue of = ValueUtils.of( collection );

        // Then
        assertThat( of, instanceOf( ListValue.class ) );
        ListValue listValue = (ListValue) of;
        assertThat( listValue.value( 0 ), equalTo( intValue( 1 ) ) );
        assertThat( listValue.value( 1 ), equalTo( intValue( 2 ) ) );
        assertThat( listValue.value( 2 ), equalTo( intValue( 3 ) ) );
        assertThat( listValue.size(), equalTo( 3 ) );
    }

    @Test
    public void shouldHandleIterator()
    {
        // Given
        Iterator<Integer> iterator = Arrays.asList( 1, 2, 3 ).iterator();

        // When
        AnyValue of = ValueUtils.of( iterator );

        // Then
        assertThat( of, instanceOf( ListValue.class ) );
        ListValue listValue = (ListValue) of;
        assertThat( listValue.value( 0 ), equalTo( intValue( 1 ) ) );
        assertThat( listValue.value( 1 ), equalTo( intValue( 2 ) ) );
        assertThat( listValue.value( 2 ), equalTo( intValue( 3 ) ) );
        assertThat( listValue.size(), equalTo( 3 ) );
    }

    @Test
    public void shouldHandleMaps()
    {
        // Given
        Map<String,Object> map = MapUtil.map( "a", Arrays.asList( "foo", 42 ) );

        // When
        AnyValue anyValue = ValueUtils.of( map );

        // Then
        assertThat( anyValue, instanceOf( MapValue.class ) );
        MapValue mapValue = (MapValue) anyValue;
        assertThat( mapValue.get( "a" ), equalTo( VirtualValues.list( stringValue( "foo" ), intValue( 42 ) ) ) );
        assertThat( mapValue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldHandleIterable()
    {
        // Given
        Iterable<Integer> collection = Arrays.asList( 1, 2, 3 );

        // When

        AnyValue of = ValueUtils.of( collection );

        // Then
        assertThat( of, instanceOf( ListValue.class ) );
        ListValue listValue = (ListValue) of;
        assertThat( listValue.value( 0 ), equalTo( intValue( 1 ) ) );
        assertThat( listValue.value( 1 ), equalTo( intValue( 2 ) ) );
        assertThat( listValue.value( 2 ), equalTo( intValue( 3 ) ) );
        assertThat( listValue.size(), equalTo( 3 ) );
    }
}
