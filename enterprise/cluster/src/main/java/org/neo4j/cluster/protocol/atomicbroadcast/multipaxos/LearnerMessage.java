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
        private final Object value;

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
            if (value instanceof Payload )
            {
                try
                {
                    ObjectStreamFactory streamFactory = new ObjectStreamFactory();
                    return new AtomicBroadcastSerializer( streamFactory, streamFactory ).receive( (Payload) value).toString();
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

            if ( value != null ? !value.equals( that.value ) : that.value != null )
            {
                return false;
            }

            return true;
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
        public LearnRequestState()
        {
        }

        @Override
        public boolean equals( Object obj )
        {
            if(obj == null)
            {
                return false;
            }
            return getClass() == obj.getClass();
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
