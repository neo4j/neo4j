/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.neo4j.kernel.impl.transaction.log.TransactionHeaders.TransactionHeadersArrayBuilder;

public class TransactionHeadersTest
{
    @Test
    public void shouldGracefullyHandleEmptyArray() throws Exception
    {
        // When
        TransactionHeaders headers = new TransactionHeaders( new byte[0] );

        // Then
        assertEquals( 0, headers.size() );
    }

    @Test
    public void shouldGracefullyHandleHeaderWithNoEntries() throws Exception
    {
        // Given
        TransactionHeadersArrayBuilder builder = new TransactionHeadersArrayBuilder();
        byte[] headerBytes = builder.build();

        // When
        TransactionHeaders headers = new TransactionHeaders( headerBytes );

        // Then
        assertEquals( 0, headers.size() );
        // since we gave no explicit version, it should use the current version
        assertEquals( TransactionHeaders.CURRENT_VERSION, headers.version() );
    }

    @Test
    public void shouldReadBackInformation() throws Exception
    {
        // Given
        TransactionHeadersArrayBuilder builder = new TransactionHeadersArrayBuilder();

        byte identifier0 = (byte) 0;
        byte[] contentFor0 = {1, 2, 3};
        builder.withHeader( identifier0, contentFor0 );

        byte nonExistingIdentifier = (byte) 1;

        byte identifier2 = (byte) 2;
        byte[] contentFor2 = {4, 5, 6};
        builder.withHeader( identifier2, contentFor2 );

        byte version = (byte) 32;
        builder.withVersion( version );

        byte[] bytes = builder.build();

        // When
        TransactionHeaders headers = new TransactionHeaders( bytes );

        // Then
        assertArrayEquals( contentFor0, headers.forIdentifier( identifier0 ) );
        assertArrayEquals( new byte[0], headers.forIdentifier( nonExistingIdentifier ) );
        assertArrayEquals( contentFor2, headers.forIdentifier( identifier2 ) );
        assertEquals( version, headers.version() );
    }
}
