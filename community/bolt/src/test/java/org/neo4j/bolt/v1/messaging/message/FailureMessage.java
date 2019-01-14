/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v1.messaging.message;

import org.neo4j.bolt.v1.messaging.BoltResponseMessageHandler;
import org.neo4j.kernel.api.exceptions.Status;

public class FailureMessage implements ResponseMessage
{
    private final Status status;
    private final String message;

    public FailureMessage( Status status, String message )
    {
        this.status = status;
        this.message = message;
    }

    public Status status()
    {
        return status;
    }

    public String message()
    {
        return message;
    }

    @Override
    public <E extends Exception> void dispatch( BoltResponseMessageHandler<E> consumer ) throws E
    {
        consumer.onFailure( status, message );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof FailureMessage) )
        {
            return false;
        }

        FailureMessage that = (FailureMessage) o;

        return (message != null ? message.equals( that.message ) : that.message == null) &&
                (status != null ? status.equals( that.status ) : that.status == null);
    }

    @Override
    public int hashCode()
    {
        int result = status != null ? status.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "FailureMessage{" +
                "status=" + status +
                ", message='" + message + '\'' +
                '}';
    }

}
