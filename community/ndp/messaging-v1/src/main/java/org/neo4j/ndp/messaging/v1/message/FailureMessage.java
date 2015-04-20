/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ndp.messaging.v1.message;

import org.neo4j.ndp.messaging.v1.MessageHandler;
import org.neo4j.ndp.runtime.internal.Neo4jError;

public class FailureMessage implements Message
{
    private final Neo4jError cause;

    public FailureMessage( Neo4jError cause )
    {
        this.cause = cause;
    }

    public Neo4jError cause()
    {
        return cause;
    }

    @Override
    public <E extends Exception> void dispatch( MessageHandler<E> consumer ) throws E
    {
        consumer.handleFailureMessage( cause );
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

        FailureMessage that = (FailureMessage) o;

        return !(cause != null ? !cause.equals( that.cause ) : that.cause != null);

    }

    @Override
    public int hashCode()
    {
        int result = 1;
        result = 31 * result + (cause != null ? cause.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "FailureMessage{" +
               ", cause=" + cause +
               '}';
    }
}
