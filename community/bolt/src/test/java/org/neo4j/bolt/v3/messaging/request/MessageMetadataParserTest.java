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
package org.neo4j.bolt.v3.messaging.request;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.messaging.BoltIOException;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.bolt.v3.messaging.request.MessageMetadataParser.parseTransactionMetadata;
import static org.neo4j.bolt.v3.messaging.request.MessageMetadataParser.parseTransactionTimeout;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.virtual.VirtualValues.emptyMap;

class MessageMetadataParserTest
{
    @Test
    void shouldAllowNoTransactionTimeout() throws Exception
    {
        assertNull( parseTransactionTimeout( emptyMap() ) );
    }

    @Test
    void shouldAllowNoTransactionMetadata() throws Exception
    {
        assertNull( parseTransactionMetadata( emptyMap() ) );
    }

    @Test
    void shouldThrowForIncorrectTransactionTimeout()
    {
        BoltIOException e = assertThrows( BoltIOException.class,
                () -> parseTransactionTimeout( asMapValue( map( "tx_timeout", "15 minutes" ) ) ) );

        assertTrue( e.causesFailureMessage() );
    }

    @Test
    void shouldThrowForIncorrectTransactionMetadata()
    {
        BoltIOException e = assertThrows( BoltIOException.class,
                () -> parseTransactionMetadata( asMapValue( map( "tx_metadata", "{key1: 'value1', key2: 'value2'}" ) ) ) );

        assertTrue( e.causesFailureMessage() );
    }
}
