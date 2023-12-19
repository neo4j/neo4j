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
 * Learner state machine messages
 */
public enum LearnerMessage
        implements MessageType
{
    join, leave,
    learn, learnRequest, learnFailed, learnTimedout, catchUp;

    public static class LearnState
            implements Serializable
    {
        private static final long serialVersionUID = 3311287172384025589L;

        private Object value;

        public LearnState( Object value )
        {
            this.value = value;
        }

        public Object getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            if ( value instanceof Payload )
            {
                try
                {
                    ObjectStreamFactory streamFactory = new ObjectStreamFactory();
                    return new AtomicBroadcastSerializer( streamFactory, streamFactory ).receive( (Payload) value )
                            .toString();
                }
                catch ( Throwable e )
                {
                    return value.toString();
                }
            }
            return value.toString();
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

            LearnState that = (LearnState) o;

            return value != null ? value.equals( that.value ) : that.value == null;
        }

        @Override
        public int hashCode()
        {
            return value != null ? value.hashCode() : 0;
        }
    }

    public static class LearnRequestState
            implements Serializable
    {
        private static final long serialVersionUID = -2577225800895578365L;

        public LearnRequestState()
        {
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj != null && getClass() == obj.getClass();
        }

        @Override
        public int hashCode()
        {
            return 1;
        }

        @Override
        public String toString()
        {
            return "Learn request";
        }
    }

    public static class LearnFailedState
            implements Serializable
    {
        private static final long serialVersionUID = -6587635550010611226L;

        public LearnFailedState()
        {
        }

        @Override
        public String toString()
        {
            return "Learn failed";
        }

        @Override
        public int hashCode()
        {
            return 0;
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof LearnFailedState;
        }
    }
}
