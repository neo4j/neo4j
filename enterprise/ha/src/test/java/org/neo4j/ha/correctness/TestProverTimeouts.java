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
package org.neo4j.ha.correctness;

import java.net.URI;

import org.junit.Test;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;

import static org.junit.Assert.assertEquals;

public class TestProverTimeouts
{

    @Test
    public void equalsShouldBeLogicalAndNotExact() throws Exception
    {
        // Given
        ProverTimeouts timeouts1 = new ProverTimeouts( new URI("http://asd") );
        ProverTimeouts timeouts2 = new ProverTimeouts( new URI("http://asd") );

        timeouts1.setTimeout( "a", Message.internal( ProposerMessage.join ) );
        timeouts1.setTimeout( "b", Message.internal( ProposerMessage.join ) );
        timeouts1.setTimeout( "c", Message.internal( ProposerMessage.join ) );

        timeouts2.setTimeout( "b", Message.internal( ProposerMessage.join ) );
        timeouts2.setTimeout( "c", Message.internal( ProposerMessage.join ) );

        // When
        timeouts1.cancelTimeout( "a" );

        // Then
        assertEquals(timeouts1, timeouts2);
    }

}
