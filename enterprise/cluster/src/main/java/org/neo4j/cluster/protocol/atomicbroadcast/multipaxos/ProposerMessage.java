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
        private static final long serialVersionUID = -7601846656107583813L;

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
            if ( toStringValue instanceof Payload )
            {
                try
                {
                    toStringValue = new AtomicBroadcastSerializer( new ObjectStreamFactory(), new ObjectStreamFactory() )
                                    .receive( (Payload) toStringValue );
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
            return value != null ? value.equals( that.value ) : that.value == null;
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
        private static final long serialVersionUID = -7876771047186067963L;

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

            return ballot == that.ballot;
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
        private static final long serialVersionUID = -7885247643311510986L;

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
        private static final long serialVersionUID = 7637829269471254916L;

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
