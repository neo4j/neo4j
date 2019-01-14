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
package org.neo4j.bolt.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.BoltResponseMessage;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.values.AnyValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertArrayEquals;

public class RecordedBoltResponse
{
    private List<QueryResult.Record> records;
    private BoltResponseMessage response;
    private Map<String, AnyValue> metadata;

    public RecordedBoltResponse()
    {
        records = new ArrayList<>();
        response = null;
        metadata = new HashMap<>();
    }

    public void addRecord( QueryResult.Record record )
    {
        records.add( record );
    }

    public void addMetadata( String key, AnyValue value )
    {
        metadata.put( key, value );
    }

    public BoltResponseMessage message()
    {
        return response;
    }

    public void setResponse( BoltResponseMessage message )
    {
        this.response = message;
    }

    public boolean hasMetadata( String key )
    {
        return metadata.containsKey( key );
    }

    public Object metadata( String key )
    {
        return metadata.get( key );
    }

    public void assertRecord( int index, Object... values )
    {
        assertThat( index, lessThan( records.size() ) );
        assertArrayEquals( records.get( index ).fields(), values );
    }

    public QueryResult.Record[] records()
    {
        QueryResult.Record[] recordArray = new QueryResult.Record[records.size()];
        return records.toArray( recordArray );
    }

    @Override
    public String toString()
    {
        return "RecordedBoltResponse{" + "records=" + records + ", response=" + response + ", metadata=" + metadata + '}';
    }
}
