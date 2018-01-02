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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.io.Serializable;

import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;

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

        @Override
        public String toString()
        {
            Object toStringValue = value;
            if (toStringValue instanceof Payload )
            {
                try
                {
                    toStringValue = new AtomicBroadcastSerializer( new ObjectStreamFactory(), new ObjectStreamFactory() ).receive( (Payload) toStringValue);
                }
                catch ( Throwable e )
                {
                    e.printStackTrace();
                }
            }

            return "PromiseState{" +
                    "ballot=" + ballot +
                    ", value=" + toStringValue +
                    '}';
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            PromiseState that = (PromiseState) o;

            if ( ballot != that.ballot )
            {
                return false;
            }
            if ( value != null ? !value.equals( that.value ) : that.value != null )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = (int) (ballot ^ (ballot >>> 32));
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
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

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            RejectPrepare that = (RejectPrepare) o;

            if ( ballot != that.ballot )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return (int) (ballot ^ (ballot >>> 32));
        }

        @Override
        public String toString()
        {
            return "RejectPrepare{" + "ballot=" + ballot + "}";
        }
    }

    public static class RejectAcceptState
            implements Serializable
    {
        public RejectAcceptState()
        {
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof RejectAcceptState;
        }

        @Override
        public int hashCode()
        {
            return 0;
        }
    }

    public static class AcceptedState
            implements Serializable
    {
        public AcceptedState()
        {
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof AcceptedState;
        }

        @Override
        public int hashCode()
        {
            return 0;
        }
    }
}
