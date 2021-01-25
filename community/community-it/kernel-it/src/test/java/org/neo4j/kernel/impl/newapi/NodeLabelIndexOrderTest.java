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

import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;

public class NodeLabelIndexOrderTest extends TokenIndexOrderTestBase<NodeLabelIndexCursor>
{

    @Override
    protected NodeLabelIndexCursor getIndexCursor( KernelTransaction tx )
    {
        return tx.cursors().allocateNodeLabelIndexCursor( tx.pageCursorTracer() );
    }

    @Override
    protected long entityReference( NodeLabelIndexCursor cursor )
    {
        return cursor.nodeReference();
    }

    @Override
    protected void tokenScan( IndexOrder indexOrder, KernelTransaction tx, int label, NodeLabelIndexCursor cursor )
    {
        tx.dataRead().nodeLabelScan( label, cursor, indexOrder );
    }

    @Override
    protected int tokenByName( KernelTransaction tx, String name )
    {
        return tx.tokenRead().nodeLabel( name );
    }

    @Override
    protected long entityWithToken( KernelTransaction tx, String name ) throws Exception
    {
        Write write = tx.dataWrite();
        long node = write.nodeCreate();
        write.nodeAddLabel( node, tx.tokenWrite().labelGetOrCreateForName( name ) );
        return node;
    }

    @Override
    protected void prepareForTokenScans( KernelTransaction tx )
    {
        tx.dataRead().prepareForLabelScans();
    }
}
