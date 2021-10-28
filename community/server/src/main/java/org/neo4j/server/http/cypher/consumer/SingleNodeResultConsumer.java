/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.http.cypher.consumer;

import java.util.function.Consumer;

import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.graphdb.Node;
import org.neo4j.server.http.cypher.CachingWriter;

public class SingleNodeResultConsumer implements ResultConsumer
{
    private final CachingWriter cachingWriter;
    private final Consumer<Node> nodeConsumer;

    public SingleNodeResultConsumer( CachingWriter cachingWriter, Consumer<Node> nodeConsumer )
    {
        this.cachingWriter = cachingWriter;
        this.nodeConsumer = nodeConsumer;
    }

    @Override
    public void consume( BoltResult result ) throws Throwable
    {
        result.handleRecords( new SingleNodeRecordConsumer( cachingWriter, nodeConsumer ), -1 );
    }

    @Override
    public boolean hasMore()
    {
        return false;
    }
}
