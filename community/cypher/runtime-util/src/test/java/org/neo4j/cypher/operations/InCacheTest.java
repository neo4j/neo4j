/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.operations;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.list;

class InCacheTest
{
    @Test
    void shouldHandleListWithNoNulls()
    {
        InCache cache = new InCache();
        ListValue list = list( stringValue( "a" ), stringValue( "b" ), stringValue( "c" ) );

        Map<Value,Value> expected =
                Map.of(
                        stringValue( "a" ), TRUE,
                        stringValue( "b" ), TRUE,
                        stringValue( "c" ), TRUE,
                        stringValue( "d" ), FALSE,
                        NO_VALUE, NO_VALUE
                );

        for ( Entry<Value,Value> entry : shuffled( expected ) )
        {
            assertThat( cache.check( entry.getKey(), list ) ).isEqualTo( entry.getValue() );
        }
    }

    @Test
    void shouldHandleListContainingNulls()
    {
        InCache cache = new InCache();
        ListValue list = list( stringValue( "a" ), NO_VALUE, stringValue( "c" ), NO_VALUE );
        Map<Value,Value> expected =
                Map.of(
                        stringValue( "a" ), TRUE,
                        stringValue( "b" ), NO_VALUE,
                        stringValue( "c" ), TRUE,
                        stringValue( "d" ), NO_VALUE,
                        NO_VALUE, NO_VALUE
                );

        for ( Entry<Value,Value> entry : shuffled( expected ) )
        {
            assertThat( cache.check( entry.getKey(), list ) ).isEqualTo( entry.getValue() );
        }
    }

    @Test
    void shouldHandleEmptyListAndNull()
    {
        //given
        InCache cache = new InCache();

        //when
        Value check = cache.check( NO_VALUE, VirtualValues.EMPTY_LIST );

        //then
        assertThat( check ).isEqualTo( FALSE );
    }

    private <K, V> Iterable<Entry<K,V>> shuffled( Map<K,V> map )
    {
        ArrayList<Entry<K,V>> list = new ArrayList<>( map.entrySet() );
        Collections.shuffle( list );
        return list;
    }
}
