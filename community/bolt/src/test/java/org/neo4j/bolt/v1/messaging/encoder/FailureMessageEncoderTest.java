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
package org.neo4j.bolt.v1.messaging.encoder;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.response.FailureMessage;
import org.neo4j.bolt.v1.messaging.response.FatalFailureMessage;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FailureMessageEncoderTest
{
    @Test
    void shouldEncodeFailureMessage() throws Throwable
    {
        // Given
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );
        Log log = mock( Log.class );
        FailureMessageEncoder encoder = new FailureMessageEncoder( log );

        // When
        QueryResult.Record value = mock( QueryResult.Record.class );
        when( value.fields() ).thenReturn( new AnyValue[0] );
        encoder.encode( packer, new FailureMessage( Status.General.UnknownError, "I am an error message" ) );

        // Then
        verify( log, never() ).debug( anyString(), any( FailureMessage.class ) );

        verify( packer ).packStructHeader( anyInt(), eq( FailureMessage.SIGNATURE ) );
        verify( packer ).packMapHeader( 2 );
        verify( packer ).pack( "code" );
        verify( packer ).pack( "message" );
    }

    @Test
    void shouldLogErrorIfIsFatalError() throws Throwable
    {
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );
        Log log = mock( Log.class );
        FailureMessageEncoder encoder = new FailureMessageEncoder( log );

        // When
        QueryResult.Record value = mock( QueryResult.Record.class );
        when( value.fields() ).thenReturn( new AnyValue[0] );
        FatalFailureMessage message = new FatalFailureMessage( Status.General.UnknownError, "I am an error message" );
        encoder.encode( packer, message );

        // Then
        verify( log ).debug( "Encoding a fatal failure message to send. Message: %s", message );
    }
}
