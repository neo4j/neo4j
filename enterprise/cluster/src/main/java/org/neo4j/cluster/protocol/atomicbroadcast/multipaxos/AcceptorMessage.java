/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.io.Serializable;

import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;

/**
 * Acceptor state machine messages
 */
public enum AcceptorMessage
        implements MessageType
{
    failure,
    join, leave,
    prepare, // phase 1a/1b
    accept; // phase 2a/2b - timeout if resulting learn is not fast enough

    public static class PrepareState
            implements Serializable
    {
        private static final long serialVersionUID = 7179066752672770593L;

        private long ballot;

        public PrepareState( long ballot )
        {
            this.ballot = ballot;
        }

        public long getBallot()
        {
            return ballot;
        }

        @Override
        public String toString()
        {
            return "PrepareState{" +
                    "ballot=" + ballot +
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

            PrepareState that = (PrepareState) o;

            return ballot == that.ballot;
        }

        @Override
        public int hashCode()
        {
            return (int) (ballot ^ (ballot >>> 32));
        }
    }

    public static class AcceptState
            implements Serializable
    {
        private static final long serialVersionUID = -5510569299948660967L;

        private long ballot;
        private Object value;

        public AcceptState( long ballot, Object value )
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

            AcceptState that = (AcceptState) o;

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

            return "AcceptState{" + "ballot=" + ballot + ", value=" + toStringValue + "}";
        }
    }
}
