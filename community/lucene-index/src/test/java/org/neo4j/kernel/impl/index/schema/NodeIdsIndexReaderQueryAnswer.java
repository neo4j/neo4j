/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.collection.PrimitiveLongResourceCollections.iterator;
import static org.neo4j.graphdb.Resource.EMPTY;
import static org.neo4j.internal.helpers.collection.Iterators.array;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.NodeValueIndexProgressor;

public class NodeIdsIndexReaderQueryAnswer implements Answer<Object> {
    private final IndexDescriptor descriptor;
    private final long[] nodeIds;

    public NodeIdsIndexReaderQueryAnswer(IndexDescriptor descriptor, long... nodeIds) {
        this.descriptor = descriptor;
        this.nodeIds = nodeIds;
    }

    @Override
    public Object answer(InvocationOnMock invocation) {
        IndexProgressor.EntityValueClient client = invocation.getArgument(0);
        NodeValueIndexProgressor progressor = new NodeValueIndexProgressor(iterator(EMPTY, nodeIds), client);
        client.initializeQuery(
                descriptor, progressor, false, false, invocation.getArgument(2), getIndexQueryArgument(invocation));
        return null;
    }

    public static PropertyIndexQuery[] getIndexQueryArgument(InvocationOnMock invocation) {
        // Apparently vararg arguments from mockito can either be non-existent, a single value or an array...
        Object rawQuery = invocation.getArgument(3);
        return rawQuery.getClass().isArray() ? (PropertyIndexQuery[]) rawQuery : array((PropertyIndexQuery) rawQuery);
    }
}
