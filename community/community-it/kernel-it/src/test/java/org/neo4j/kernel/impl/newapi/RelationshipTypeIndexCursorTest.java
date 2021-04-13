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

import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.AnyTokens;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class RelationshipTypeIndexCursorTest extends RelationshipTypeIndexCursorTestBase<WriteTestSupport>
{
    @Override
    public WriteTestSupport newTestSupport()
    {
        return new WriteTestSupport()
        {
            @Override
            protected TestDatabaseManagementServiceBuilder configure( TestDatabaseManagementServiceBuilder builder )
            {
                builder = builder.setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
                return super.configure( builder );
            }
        };
    }

    @BeforeEach
    public void setupRelTypeIndex()
    {
        // KernelAPIWriteTestBase removes all indexes after creating the database so need to recreate the relation type index.
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( AnyTokens.ANY_RELATIONSHIP_TYPES ).withName( "rti" ).create();
            tx.commit();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
        }
    }
}
