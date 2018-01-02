/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.bolt.v1.messaging.BoltResponseMessageHandler;
import org.neo4j.cypher.result.QueryResult;

public class RecordMessage implements ResponseMessage
{
    private final QueryResult.Record value;

    public RecordMessage( QueryResult.Record record )
    {
        this.value = record;
    }

    @Override
    public <E extends Exception> void dispatch( BoltResponseMessageHandler<E> consumer ) throws E
    {
        consumer.onRecord( value );
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

        RecordMessage that = (RecordMessage) o;

        return value == null ? that.value == null : value.equals( that.value );
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public String toString()
    {
        return "ItemMessage{" +
               "datas=" + value +
               '}';
    }

    public QueryResult.Record record()
    {
        return value;
    }
}
