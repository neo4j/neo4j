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
import org.neo4j.values.virtual.MapValue;

public class SuccessMessage implements ResponseMessage
{
    private final MapValue metadata;

    public SuccessMessage( MapValue metadata )
    {
        this.metadata = metadata;
    }

    @Override
    public <E extends Exception> void dispatch( BoltResponseMessageHandler<E> consumer ) throws E
    {
        consumer.onSuccess( metadata );
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

        SuccessMessage that = (SuccessMessage) o;

        return metadata.equals( that.metadata );

    }

    @Override
    public int hashCode()
    {
        return metadata.hashCode();
    }

    @Override
    public String toString()
    {
        return "SuccessMessage{" +
               "metadata=" + metadata +
               '}';
    }

    public MapValue meta()
    {
        return metadata;
    }
}
