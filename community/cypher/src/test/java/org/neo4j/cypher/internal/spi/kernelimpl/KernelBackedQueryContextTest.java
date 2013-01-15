/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.kernelimpl;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.StatementContext;

import java.util.Arrays;

import static org.mockito.Mockito.*;

public class KernelBackedQueryContextTest {

    @Test
    public void shouldAddLabelsToNode()
    {
        // Given
        Node mockNode = mock(Node.class);
        when(mockNode.getId()).thenReturn(1337l);
        GraphDatabaseService graph = mock(GraphDatabaseService.class);
        StatementContext mockCtx = mock(StatementContext.class);

        KernelBackedQueryContext ctx = new KernelBackedQueryContext(graph, mockCtx);

        // When
        ctx.addLabelsToNode(mockNode, Arrays.asList(1l,2l,3l));

        // Then
        verify(mockCtx).addLabelToNode(1l, 1337l);
        verify(mockCtx).addLabelToNode(2l, 1337l);
        verify(mockCtx).addLabelToNode(3l, 1337l);
    }

    @Test
    public void shouldGetOrCreateLabelId()
    {
        // Given
        StatementContext mockCtx = mock(StatementContext.class);
        when(mockCtx.getOrCreateLabelId("labelA")).thenReturn(23l);

        GraphDatabaseService graph = mock(GraphDatabaseService.class);
        KernelBackedQueryContext ctx = new KernelBackedQueryContext(graph, mockCtx);

        // When
        final Long result = ctx.getOrCreateLabelId("labelA");

        // Then
        verify(mockCtx).getOrCreateLabelId("labelA");
        assert result == 23l;
    }
}
