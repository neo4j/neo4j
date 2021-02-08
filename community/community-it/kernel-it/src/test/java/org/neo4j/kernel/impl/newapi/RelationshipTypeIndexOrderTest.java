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

import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class RelationshipTypeIndexOrderTest extends TokenIndexOrderTestBase<RelationshipTypeIndexCursor>
{
    @Override
    public WriteTestSupport newTestSupport()
    {
        return new WriteTestSupport()
        {
            @Override
            protected TestDatabaseManagementServiceBuilder configure( TestDatabaseManagementServiceBuilder builder )
            {
                builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );
                return super.configure( builder );
            }
        };
    }

    @Override
    protected long entityWithToken( KernelTransaction tx, String name ) throws Exception
    {
        Write write = tx.dataWrite();
        long sourceNode = write.nodeCreate();
        long targetNode = write.nodeCreate();

        return write.relationshipCreate( sourceNode, tx.tokenWrite().relationshipTypeGetOrCreateForName( name ), targetNode );
    }

    @Override
    protected RelationshipTypeIndexCursor getIndexCursor( KernelTransaction tx )
    {
        return tx.cursors().allocateRelationshipTypeIndexCursor( tx.pageCursorTracer() );
    }

    @Override
    protected long entityReference( RelationshipTypeIndexCursor cursor )
    {
        return cursor.relationshipReference();
    }

    @Override
    protected void tokenScan( IndexOrder indexOrder, KernelTransaction tx, int label, RelationshipTypeIndexCursor cursor )
    {
        tx.dataRead().relationshipTypeScan( label, cursor, indexOrder );
    }

    @Override
    protected int tokenByName( KernelTransaction tx, String name )
    {
        return tx.tokenRead().relationshipType( name );
    }

    @Override
    protected void prepareForTokenScans( KernelTransaction tx )
    {
    }

}
