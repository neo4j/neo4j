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

import org.neo4j.bolt.v1.messaging.BoltRequestMessageHandler;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

public class RunMessage implements RequestMessage
{
    private static final MapValue EMPTY_PARAMETERS = VirtualValues.EMPTY_MAP;

    /**
     * Factory method for obtaining RUN messages.
     */
    public static RunMessage run( String statement, MapValue parameters )
    {
        return new RunMessage( statement, parameters );
    }

    /**
     * Factory method for obtaining RUN messages with no parameters.
     */

    public static RunMessage run( String statement )
    {
        return run( statement, EMPTY_PARAMETERS );
    }

    private final String statement;
    private final MapValue params;

    private RunMessage( String statement, MapValue params )
    {
        this.statement = statement;
        this.params = params;
    }

    public MapValue params()
    {
        return params;
    }

    public String statement()
    {
        return statement;
    }

    @Override
    public void dispatch( BoltRequestMessageHandler consumer )
    {
        consumer.onRun( statement, params );
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

        return params.equals( that.params ) && statement.equals( that.statement );

    }

    @Override
    public int hashCode()
    {
        int result = statement.hashCode();
        result = 31 * result + params.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "RunMessage{" +
                "statement='" + statement + '\'' +
                ", params=" + params +
                '}';
    }
}
