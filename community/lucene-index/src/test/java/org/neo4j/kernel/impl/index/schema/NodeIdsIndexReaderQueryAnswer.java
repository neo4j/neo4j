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
package org.neo4j.kernel.impl.index.schema;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.NodeValueIndexProgressor;

import static org.neo4j.collection.PrimitiveLongResourceCollections.iterator;
import static org.neo4j.graphdb.Resource.EMPTY;
import static org.neo4j.internal.helpers.collection.Iterators.array;

public class NodeIdsIndexReaderQueryAnswer implements Answer
{
    private final IndexDescriptor descriptor;
    private final long[] nodeIds;

    public NodeIdsIndexReaderQueryAnswer( IndexDescriptor descriptor, long... nodeIds )
    {
        this.descriptor = descriptor;
        this.nodeIds = nodeIds;
    }

    @Override
    public Object answer( InvocationOnMock invocation )
    {
        IndexProgressor.EntityValueClient client = invocation.getArgument( 1 );
        NodeValueIndexProgressor progressor = new NodeValueIndexProgressor( iterator( EMPTY, nodeIds ), client );
        client.initialize( descriptor, progressor, getIndexQueryArgument( invocation ), invocation.getArgument( 2 ), invocation.getArgument( 3 ), false );
        return null;
    }

    public static IndexQuery[] getIndexQueryArgument( InvocationOnMock invocation )
    {
        // Apparently vararg arguments from mockitor can either be non-existent, a single value or an array...
        Object rawQuery = invocation.getArgument( 4 );
        return rawQuery.getClass().isArray() ? (IndexQuery[]) rawQuery : array( (IndexQuery) rawQuery );
    }
}
