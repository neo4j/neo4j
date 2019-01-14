/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
