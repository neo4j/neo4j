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
package org.neo4j.bolt.v3.messaging.request;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public abstract class AbstractTransactionInitiatingMessage
{
    @Test
    void shouldParseEmptyTransactionMetadataCorrectly() throws Throwable
    {
        // When
        TransactionInitiatingMessage message = createMessage();

        // Then
        assertEquals( Map.of(), message.transactionMetadata() );
    }

    @Test
    void shouldThrowExceptionIfFailedToParseTransactionMetadataCorrectly() throws Throwable
    {
        // Given
        Map<String,Object> msgMetadata = map( "tx_metadata", "invalid value type" );
        MapValue meta = ValueUtils.asMapValue( msgMetadata );
        // When & Then
        BoltIOException exception = assertThrows( BoltIOException.class, () -> createMessage( meta ) );
        assertThat( exception.getMessage(), startsWith( "Expecting transaction metadata value to be a Map value" ) );
    }

    @Test
    void shouldParseTransactionMetadataCorrectly() throws Throwable
    {
        // Given
        Map<String,Object> txMetadata = map( "creation-time", Duration.ofMillis( 4321L ) );
        Map<String,Object> msgMetadata = map( "tx_metadata", txMetadata );
        MapValue meta = ValueUtils.asMapValue( msgMetadata );

        // When
        TransactionInitiatingMessage beginMessage = createMessage(meta);

        // Then
        assertThat( beginMessage.transactionMetadata().toString(), equalTo( txMetadata.toString() ) );
    }

    @Test
    void shouldThrowExceptionIfFailedToParseTransactionTimeoutCorrectly() throws Throwable
    {
        // Given
        Map<String,Object> msgMetadata = map( "tx_timeout", "invalid value type" );
        MapValue meta = ValueUtils.asMapValue( msgMetadata );
        // When & Then
        BoltIOException exception = assertThrows( BoltIOException.class, () -> createMessage( meta ) );
        assertThat( exception.getMessage(), startsWith( "Expecting transaction timeout value to be a Long value" ) );
    }

    @Test
    void shouldParseTransactionTimeoutCorrectly() throws Throwable
    {
        // Given
        Map<String,Object> msgMetadata = map( "tx_timeout", 123456L );
        MapValue meta = ValueUtils.asMapValue( msgMetadata );

        // When
        TransactionInitiatingMessage beginMessage = createMessage( meta );

        // Then
        assertThat( beginMessage.transactionTimeout().toMillis(), equalTo( 123456L ) );
    }

    @Test
    void shouldThrowExceptionIfFailedToParseAccessModeCorrectly() throws Throwable
    {
        // Given
        Map<String,Object> msgMetadata = map( "mode", 666 );
        MapValue meta = ValueUtils.asMapValue( msgMetadata );
        // When & Then
        BoltIOException exception = assertThrows( BoltIOException.class, () -> createMessage( meta ) );
        assertThat( exception.getMessage(), startsWith( "Expecting access mode value to be a String value" ) );
    }

    @Test
    void shouldParseNoAccessModeCorrectly() throws Throwable
    {
        // Given
        Map<String,Object> msgMetadata = map();
        MapValue meta = ValueUtils.asMapValue( msgMetadata );

        // When
        TransactionInitiatingMessage beginMessage = createMessage( meta );

        // Then
        assertThat( beginMessage.getAccessMode(), equalTo( AccessMode.WRITE ) );
    }

    @Test
    void shouldParseReadAccessModeCorrectly() throws Throwable
    {
        // Given
        Map<String,Object> msgMetadata = map("mode", "r" );
        MapValue meta = ValueUtils.asMapValue( msgMetadata );

        // When
        TransactionInitiatingMessage beginMessage = createMessage( meta );

        // Then
        assertThat( beginMessage.getAccessMode(), equalTo( AccessMode.READ ) );
    }

    @Test
    void shouldParseWriteAccessModeCorrectly() throws Throwable
    {
        // Given
        Map<String,Object> msgMetadata = map("mode", "w" );
        MapValue meta = ValueUtils.asMapValue( msgMetadata );

        // When
        TransactionInitiatingMessage beginMessage = createMessage( meta );

        // Then
        assertThat( beginMessage.getAccessMode(), equalTo( AccessMode.WRITE ) );
    }

    protected abstract TransactionInitiatingMessage createMessage() throws IOException;

    protected abstract TransactionInitiatingMessage createMessage( MapValue meta ) throws IOException;
}
