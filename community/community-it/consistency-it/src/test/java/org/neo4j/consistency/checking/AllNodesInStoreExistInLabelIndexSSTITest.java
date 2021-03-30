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
package org.neo4j.consistency.checking;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.RandomExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;

@DbmsExtension( configurationCallback = "configure" )
@ExtendWith( RandomExtension.class )
class AllNodesInStoreExistInLabelIndexSSTITest extends AllNodesInStoreExistInLabelIndexTest
{
    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
    }

    @Override
    Config addAdditionalConfigToCC( Config config )
    {
        config.set( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
        return config;
    }

    @Test
    void checkShouldBeSuccessfulIfNoNodeLabelIndexExist() throws ConsistencyCheckIncompleteException
    {
        // Remove the Node Label Index
        try ( Transaction tx = db.beginTx() )
        {
            final Iterable<IndexDefinition> indexes = tx.schema().getIndexes();
            for ( IndexDefinition index : indexes )
            {
                if ( index.getIndexType() == IndexType.LOOKUP && index.isNodeIndex() )
                {
                    index.drop();
                }
            }
            tx.commit();
        }

        // Add some data to the node store
        someData();
        managementService.shutdown();

        // Then consistency check should still be successful without NLI
        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertTrue( result.isSuccessful(), "Expected consistency check to succeed" );
    }
}
