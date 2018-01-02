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
package org.neo4j.graphdb.impl.notification;

import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class NotificationDetailTest
{
    @Test
    public void shouldConstructIndexDetails()
    {
        NotificationDetail detail = NotificationDetail.Factory.index( "Person", "name" );

        assertThat( detail.name(), equalTo( "hinted index" ) );
        assertThat( detail.value(), equalTo( "index on :Person(name)" ) );
        assertThat( detail.toString(), equalTo( "hinted index is: index on :Person(name)" ) );
    }

    @Test
    public void shouldConstructCartesianProductDetailsSingular()
    {
        Set<String> idents = new HashSet<>();
        idents.add( "n" );
        NotificationDetail detail = NotificationDetail.Factory.cartesianProduct( idents );

        assertThat( detail.name(), equalTo( "identifier" ) );
        assertThat( detail.value(), equalTo( "(n)" ) );
        assertThat( detail.toString(), equalTo( "identifier is: (n)" ) );
    }

    @Test
    public void shouldConstructCartesianProductDetails()
    {
        Set<String> idents = new TreeSet<>();
        idents.add( "n" );
        idents.add( "node2" );
        NotificationDetail detail = NotificationDetail.Factory.cartesianProduct( idents );

        assertThat( detail.name(), equalTo( "identifiers" ) );
        assertThat( detail.value(), equalTo( "(n, node2)" ) );
        assertThat( detail.toString(), equalTo( "identifiers are: (n, node2)" ) );
    }

    @Test
    public void shouldConstructJoinHintDetailsSingular()
    {
        List<String> idents = new ArrayList<>();
        idents.add( "n" );
        NotificationDetail detail = NotificationDetail.Factory.joinKey( idents );

        assertThat( detail.name(), equalTo( "hinted join key identifier" ) );
        assertThat( detail.value(), equalTo( "n" ) );
        assertThat( detail.toString(), equalTo( "hinted join key identifier is: n" ) );
    }

    @Test
    public void shouldConstructJoinHintDetails()
    {
        List<String> idents = new ArrayList<>();
        idents.add( "n" );
        idents.add( "node2" );
        NotificationDetail detail = NotificationDetail.Factory.joinKey( idents );

        assertThat( detail.name(), equalTo( "hinted join key identifiers" ) );
        assertThat( detail.value(), equalTo( "n, node2" ) );
        assertThat( detail.toString(), equalTo( "hinted join key identifiers are: n, node2" ) );
    }
}
