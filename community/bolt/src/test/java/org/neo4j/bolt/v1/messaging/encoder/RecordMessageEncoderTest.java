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
import org.neo4j.bolt.v1.messaging.response.RecordMessage;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.values.AnyValue;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RecordMessageEncoderTest
{
    @Test
    void shouldEncodeRecordMessage() throws Throwable
    {
        // Given
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );
        RecordMessageEncoder encoder = new RecordMessageEncoder();

        // When
        QueryResult.Record value = mock( QueryResult.Record.class );
        when( value.fields() ).thenReturn( new AnyValue[0] );
        encoder.encode( packer, new RecordMessage( value ) );

        // Then
        verify( packer ).packStructHeader( anyInt(), eq( RecordMessage.SIGNATURE ) );
        verify( packer ).packListHeader( 0 );
    }
}
