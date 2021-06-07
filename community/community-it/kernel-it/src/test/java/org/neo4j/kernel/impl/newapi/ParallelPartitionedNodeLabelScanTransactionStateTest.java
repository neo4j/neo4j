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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.BeforeAll;

import org.neo4j.exceptions.KernelException;

import static org.neo4j.kernel.impl.newapi.ParallelPartitionedNodeLabelScanTest.NUMBER_OF_NODES;

public class ParallelPartitionedNodeLabelScanTransactionStateTest
        extends ParallelPartitionedNodeLabelScanTransactionStateTestBase<WriteTestSupport>
{
    @Override
    public WriteTestSupport newTestSupport()
    {
        return new WriteTestSupport();
    }

    @BeforeAll
    public void createTestGraph()
    {
        final var labels = Label.values();
        try ( var tx = beginTransaction() )
        {
            final var write = tx.dataWrite();
            for ( int i = 0; i < NUMBER_OF_NODES; i++ )
            {
                final var node = write.nodeCreate();
                final var label = labels[i % labels.length];
                write.nodeAddLabel( node, LABEL_IDS.get( label ) );
            }
            tx.commit();
        }
        catch ( KernelException e )
        {
            throw new AssertionError( e );
        }
    }
}
