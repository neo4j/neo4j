/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.catchup.tx;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;

import org.neo4j.causalclustering.messaging.EventHandler;

import static org.junit.Assert.assertTrue;

class AssertableEventHandler implements EventHandler
{
    private final LinkedList<EventContext> exectedEventContext = new LinkedList<>();
    private final LinkedList<EventContext> givenNonMatchingEventContext = new LinkedList<>();
    private boolean assertMode = true;

    void setAssertMode()
    {
        assertMode = true;
    }

    void setVerifyingMode()
    {
        assertMode = false;
    }

    @Override
    public void on( EventState eventState, String message, Throwable throwable, Param... params )
    {
        EventContext eventContext = new EventContext( eventState, message, throwable, params );
        if ( assertMode )
        {
            exectedEventContext.add( eventContext );
        }
        else
        {
            if ( !exectedEventContext.remove( eventContext ) )
            {
                givenNonMatchingEventContext.add( eventContext );
            }
        }
    }

    void assertAllFound()
    {
        assertTrue( "Still contains asserted event: " + Arrays.toString( exectedEventContext.toArray( new EventContext[0] ) ) + ". Got: " +
                Arrays.toString( givenNonMatchingEventContext.toArray( new EventContext[0] ) ), exectedEventContext.isEmpty() );
    }

    class EventContext
    {
        private final EventState eventState;
        private final String message;
        private final Throwable throwable;
        private final Param[] params;

        EventContext( EventState eventState, String message, Throwable throwable, Param[] params )
        {

            this.eventState = eventState;
            this.message = message;
            this.throwable = throwable;
            this.params = params;
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
            EventContext that = (EventContext) o;
            boolean arrayqual = Arrays.equals( params, that.params );
            boolean stateEqual = eventState == that.eventState;
            boolean messageEqual = Objects.equals( message, that.message );
            boolean throwableEquals = Objects.equals( throwable, that.throwable );
            return stateEqual && messageEqual && throwableEquals && arrayqual;
        }

        @Override
        public int hashCode()
        {

            int result = Objects.hash( eventState, message, throwable );
            result = 31 * result + Arrays.hashCode( params );
            return result;
        }

        @Override
        public String toString()
        {
            return "EventContext{" + "eventState=" + eventState + ", message='" + message + '\'' + ", throwable=" + throwable + ", params=" +
                    Arrays.toString( params ) + '}';
        }
    }
}
