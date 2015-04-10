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
import java.util.Map;

import org.neo4j.driver.Value;

import static java.lang.String.format;

/**
 * RUN request message
 * <p>
 * Sent by clients to start a new Tank job for a given statement and
 * parameter set.
 */
public class RunMessage implements Message
{
    private final String statement;
    private final Map<String,Value> parameters;

    public RunMessage( String statement, Map<String,Value> parameters )
    {
        this.statement = statement;
        this.parameters = parameters;
    }

    @Override
    public void dispatch( MessageHandler handler ) throws IOException
    {
        handler.handleRunMessage( statement, parameters );
    }

    @Override
    public String toString()
    {
        return format( "[RUN \"%s\" %s]", statement, parameters );
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

        RunMessage that = (RunMessage) o;

        if ( parameters != null ? !parameters.equals( that.parameters ) : that.parameters != null )
        {
            return false;
        }
        if ( statement != null ? !statement.equals( that.statement ) : that.statement != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = statement != null ? statement.hashCode() : 0;
        result = 31 * result + (parameters != null ? parameters.hashCode() : 0);
        return result;
    }
}
