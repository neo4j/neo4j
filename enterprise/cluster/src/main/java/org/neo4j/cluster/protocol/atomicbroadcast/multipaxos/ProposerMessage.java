/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.io.Serializable;

import org.neo4j.cluster.com.message.MessageType;

/**
 * Coordinator state machine messages
 */
public enum ProposerMessage
        implements MessageType
{
    join, leave,
    phase1Timeout,
    propose, // If no accept message is sent out, it means not enough promises have come in
    promise, rejectPrepare, rejectPropose2, // phase 1b
    phase2Timeout,
    accepted, rejectAccept; // phase 2b

    public static class PromiseState
            implements Serializable
    {
        private long ballot;
        private Object value;

        public PromiseState( long ballot, Object value )
        {
            this.ballot = ballot;
            this.value = value;
        }

        public long getBallot()
        {
            return ballot;
        }

        public Object getValue()
        {
            return value;
        }
    }

    public static class RejectPrepare
            implements Serializable
    {
        private long ballot;

        public RejectPrepare( long ballot )
        {
            this.ballot = ballot;
        }

        public long getBallot()
        {
            return ballot;
        }
    }

    public static class RejectAcceptState
            implements Serializable
    {
        public RejectAcceptState()
        {
        }
    }

    public static class AcceptedState
            implements Serializable
    {
        public AcceptedState()
        {
        }
    }
}
