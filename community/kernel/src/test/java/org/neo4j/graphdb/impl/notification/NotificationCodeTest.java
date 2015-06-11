/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.SeverityLevel;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.impl.notification.NotificationCode.INDEX_HINT_UNFULFILLABLE;

public class NotificationCodeTest
{
    @Test
    public void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE()
    {
        NotificationDetail indexDetail = NotificationDetail.Factory.index( "hinted index", "Person", "name" );
        Notification notification = INDEX_HINT_UNFULFILLABLE.notification( InputPosition.empty, indexDetail );

        assertThat( notification.getTitle(), equalTo( "The request (directly or indirectly) referred to an index that does not exist." ) );
        assertThat( notification.getSeverity(), equalTo( SeverityLevel.WARNING ) );
        assertThat( notification.getCode(), equalTo( "Neo.ClientError.Schema.NoSuchIndex" ) );
        assertThat( notification.getPosition(), equalTo( InputPosition.empty ) );
        assertThat( notification.getDescription(), equalTo( "The hinted index does not exist, please check the schema (hinted index is index on :Person(name))" ) );
    }

}
