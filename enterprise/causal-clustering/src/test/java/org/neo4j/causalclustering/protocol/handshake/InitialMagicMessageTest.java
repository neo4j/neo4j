/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.protocol.handshake;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InitialMagicMessageTest
{
    @Test
    public void shouldCreateWithCorrectMagicValue()
    {
        // given
        InitialMagicMessage magicMessage = InitialMagicMessage.instance();

        // then
        assertTrue( magicMessage.isCorrectMagic() );
        assertEquals( "NEO4J_CLUSTER", magicMessage.magic() );
    }

    @Test
    public void shouldHaveCorrectMessageCode() throws Exception
    {
        byte[] bytes = InitialMagicMessage.CORRECT_MAGIC_VALUE.substring( 0, 4 ).getBytes( "UTF-8" );
        int messageCode = bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);

        assertEquals( 0x344F454E, messageCode );
        assertEquals( 0x344F454E, InitialMagicMessage.MESSAGE_CODE );
    }
}
