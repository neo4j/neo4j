/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal.messaging;

import java.io.IOException;

import static java.lang.String.format;

/**
 * FAILURE response message
 * <p>
 * Sent by the server to signal a failed operation.
 * Terminates response sequence.
 */
public class FailureMessage implements Message
{
    private final String code;
    private final String message;

    public FailureMessage( String code, String message )
    {
        super();
        this.code = code;
        this.message = message;
    }

    @Override
    public void dispatch( MessageHandler handler ) throws IOException
    {
        handler.handleFailureMessage( code, message );
    }

    @Override
    public String toString()
    {
        return format( "[FAILURE %s \"%s\"]", code, message );
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

        if ( code != null ? !code.equals( that.code ) : that.code != null )
        {
            return false;
        }
        if ( message != null ? !message.equals( that.message ) : that.message != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = code != null ? code.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }
}
