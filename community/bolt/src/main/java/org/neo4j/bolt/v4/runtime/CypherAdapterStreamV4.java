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
package org.neo4j.bolt.v4.runtime;

import java.time.Clock;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.bolt.v3.runtime.CypherAdapterStreamV3;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.values.AnyValue;

/**
 * This is a minimal effort of work to adopt the cypher result visitor API to the new PULL_N pattern.
 */
public class CypherAdapterStreamV4 extends CypherAdapterStreamV3
{
    private final Queue<QueryResult.Record> allRecords;

    public CypherAdapterStreamV4( QueryResult delegate, Clock clock )
    {
        super( delegate, clock );
        this.allRecords = new LinkedList<>();
        pullInAllRecords();
    }

    @Override
    public boolean handleRecords( final Visitor visitor, long size ) throws Exception
    {
        while ( size-- > 0 )
        {
            QueryResult.Record current = allRecords.poll();

            if ( current != null )
            {
                visitor.visit( current );
            }
            // if we are at the end of the queue
            if ( current == null || allRecords.peek() == null )
            {
                metadata.forEach( visitor::addMetadata );
                return false;
            }
        }
        return true;
    }

    private void pullInAllRecords()
    {
        long start = clock.millis();
        delegate.accept( row -> {
            AnyValue[] src = row.fields();
            AnyValue[] dest = new AnyValue[src.length];
            System.arraycopy( src, 0, dest, 0, src.length );
            allRecords.add( () -> dest );
            return true;
        } );
        addRecordStreamingTime( clock.millis() - start );
        addMetadata();
    }
}
