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
package org.neo4j.bolt.v4.messaging;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.messaging.BoltIOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.parseDatabaseName;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

class MessageMetadataParserTest
{
    @Test
    void noDatabaseNameShouldDefaultToEmptyString() throws Exception
    {
        assertThat( ABSENT_DB_NAME, equalTo( "" ) );
        assertThat( parseDatabaseName( EMPTY_MAP ), equalTo( ABSENT_DB_NAME ) );
    }

    @Test
    void shouldParseDatabaseName() throws Exception
    {
        String databaseName = "cat_pictures";
        assertThat( parseDatabaseName( asMapValue( map( "db", databaseName ) ) ), equalTo( databaseName ) );
    }

    @Test
    void shouldThrowForIncorrectDatabaseName()
    {
        BoltIOException e = assertThrows( BoltIOException.class,
                () -> parseDatabaseName( asMapValue( map( "db", 10L ) ) ) );

        assertTrue( e.causesFailureMessage() );
    }
}
