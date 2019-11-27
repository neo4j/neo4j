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
package org.neo4j.graphdb.impl.notification;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationDetailTest
{
    @Test
    void shouldConstructIndexDetails()
    {
        NotificationDetail detail = NotificationDetail.Factory.index( "Person", "name" );

        assertThat( detail.name() ).isEqualTo( "hinted index" );
        assertThat( detail.value() ).isEqualTo( "index on :Person(name)" );
        assertThat( detail.toString() ).isEqualTo( "hinted index is: index on :Person(name)" );
    }

    @Test
    void shouldConstructSuboptimalIndexDetails()
    {
        NotificationDetail detail = NotificationDetail.Factory.suboptimalIndex( "Person", "name" );

        assertThat( detail.name() ).isEqualTo( "index" );
        assertThat( detail.value() ).isEqualTo( "index on :Person(name)" );
        assertThat( detail.toString() ).isEqualTo( "index is: index on :Person(name)" );
    }

    @Test
    void shouldConstructCartesianProductDetailsSingular()
    {
        Set<String> idents = new HashSet<>();
        idents.add( "n" );
        NotificationDetail detail = NotificationDetail.Factory.cartesianProduct( idents );

        assertThat( detail.name() ).isEqualTo( "identifier" );
        assertThat( detail.value() ).isEqualTo( "(n)" );
        assertThat( detail.toString() ).isEqualTo( "identifier is: (n)" );
    }

    @Test
    void shouldConstructCartesianProductDetails()
    {
        Set<String> idents = new TreeSet<>();
        idents.add( "n" );
        idents.add( "node2" );
        NotificationDetail detail = NotificationDetail.Factory.cartesianProduct( idents );

        assertThat( detail.name() ).isEqualTo( "identifiers" );
        assertThat( detail.value() ).isEqualTo( "(n, node2)" );
        assertThat( detail.toString() ).isEqualTo( "identifiers are: (n, node2)" );
    }

    @Test
    void shouldConstructJoinHintDetailsSingular()
    {
        List<String> idents = new ArrayList<>();
        idents.add( "n" );
        NotificationDetail detail = NotificationDetail.Factory.joinKey( idents );

        assertThat( detail.name() ).isEqualTo( "hinted join key identifier" );
        assertThat( detail.value() ).isEqualTo( "n" );
        assertThat( detail.toString() ).isEqualTo( "hinted join key identifier is: n" );
    }

    @Test
    void shouldConstructJoinHintDetails()
    {
        List<String> idents = new ArrayList<>();
        idents.add( "n" );
        idents.add( "node2" );
        NotificationDetail detail = NotificationDetail.Factory.joinKey( idents );

        assertThat( detail.name() ).isEqualTo( "hinted join key identifiers" );
        assertThat( detail.value() ).isEqualTo( "n, node2" );
        assertThat( detail.toString() ).isEqualTo( "hinted join key identifiers are: n, node2" );
    }
}
