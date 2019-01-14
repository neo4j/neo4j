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
package org.neo4j.bolt.v1.messaging.encoder;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.response.IgnoredMessage;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.v1.messaging.response.IgnoredMessage.IGNORED_MESSAGE;

class IgnoredMessageEncoderTest
{
    @Test
    void shouldEncodeIgnoredMessage() throws Throwable
    {
        // Given
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );
        IgnoredMessageEncoder encoder = new IgnoredMessageEncoder();

        // When
        encoder.encode( packer, IGNORED_MESSAGE );

        // Then
        verify( packer ).packStructHeader( anyInt(), eq( IgnoredMessage.SIGNATURE ) );
    }
}
