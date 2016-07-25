/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.messaging.message;

import java.util.Map;

import org.neo4j.bolt.v1.messaging.MessageHandler;

public class SuccessMessage implements Message
{
    private final Map<String,Object> metadata;

    public SuccessMessage( Map<String,Object> metadata )
    {
        this.metadata = metadata;
    }

    @Override
    public <E extends Exception> void dispatch( MessageHandler<E> consumer ) throws E
    {
        consumer.handleSuccessMessage( metadata );
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

    public Map<String,Object> meta()
    {
        return metadata;
    }
}
