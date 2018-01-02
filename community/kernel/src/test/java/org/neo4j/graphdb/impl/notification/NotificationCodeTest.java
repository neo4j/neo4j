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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.SeverityLevel;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.impl.notification.NotificationCode.CARTESIAN_PRODUCT;
import static org.neo4j.graphdb.impl.notification.NotificationCode.INDEX_HINT_UNFULFILLABLE;
import static org.neo4j.graphdb.impl.notification.NotificationCode.JOIN_HINT_UNFULFILLABLE;

public class NotificationCodeTest
{
    @Test
    public void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE()
    {
        NotificationDetail indexDetail = NotificationDetail.Factory.index( "Person", "name" );
        Notification notification = INDEX_HINT_UNFULFILLABLE.notification( InputPosition.empty, indexDetail );

        assertThat( notification.getTitle(), equalTo( "The request (directly or indirectly) referred to an index that does not exist." ) );
        assertThat( notification.getSeverity(), equalTo( SeverityLevel.WARNING ) );
        assertThat( notification.getCode(), equalTo( "Neo.ClientError.Schema.NoSuchIndex" ) );
        assertThat( notification.getPosition(), equalTo( InputPosition.empty ) );
        assertThat( notification.getDescription(), equalTo( "The hinted index does not exist, please check the schema (hinted index is: index on :Person(name))" ) );
    }

    @Test
    public void shouldConstructNotificationFor_CARTESIAN_PRODUCT()
    {
        Set<String> idents = new TreeSet<>();
        idents.add( "n" );
        idents.add( "node2" );
        NotificationDetail identifierDetail = NotificationDetail.Factory.cartesianProduct( idents );
        Notification notification = CARTESIAN_PRODUCT.notification( InputPosition.empty, identifierDetail );

        assertThat( notification.getTitle(), equalTo( "This query builds a cartesian product between disconnected patterns." ) );
        assertThat( notification.getSeverity(), equalTo( SeverityLevel.WARNING ) );
        assertThat( notification.getCode(), equalTo( "Neo.ClientNotification.Statement.CartesianProduct" ) );
        assertThat( notification.getPosition(), equalTo( InputPosition.empty ) );
        assertThat( notification.getDescription(), equalTo( "If a part of a query contains multiple disconnected patterns, this will build a cartesian product " +
                                                            "between all those parts. This may produce a large amount of data and slow down query processing. While " +
                                                            "occasionally intended, it may often be possible to reformulate the query that avoids the use of this cross " +
                                                            "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH " +
                                                            "(identifiers are: (n, node2))" ) );
    }

    @Test
    public void shouldConstructNotificationsFor_JOIN_HINT_UNFULFILLABLE() {
        List<String> idents = new ArrayList<>();
        idents.add( "n" );
        idents.add( "node2" );
        NotificationDetail identifierDetail = NotificationDetail.Factory.joinKey(idents);
        Notification notification = JOIN_HINT_UNFULFILLABLE.notification( InputPosition.empty, identifierDetail );

        assertThat( notification.getTitle(), equalTo( "The database was unable to plan a hinted join." ) );
        assertThat( notification.getSeverity(), equalTo( SeverityLevel.WARNING ) );
        assertThat( notification.getCode(), equalTo( "Neo.ClientNotification.Statement.JoinHintUnfulfillableWarning" ) );
        assertThat( notification.getPosition(), equalTo( InputPosition.empty ) );
        assertThat( notification.getDescription(),
            equalTo( "The hinted join was not planned. This could happen because no generated plan contained the join key, " +
                     "please try using a different join key or restructure your query. " +
                     "(hinted join key identifiers are: n, node2)" ) );
    }
}
