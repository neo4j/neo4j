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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class TestTxEntries
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void testStartEntryWrittenOnceOnRollback()
    {
        File storeDir = testDirectory.storeDir();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( storeDir )
                .setFileSystem( fs )
                .impermanent()
                .build();
        final GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        createSomeTransactions( db );
        EphemeralFileSystemAbstraction snapshot = fs.snapshot();
        managementService.shutdown();

        managementService = new TestDatabaseManagementServiceBuilder( storeDir )
                .setFileSystem( snapshot )
                .impermanent()
                .build();
        managementService.shutdown();
    }

    private void createSomeTransactions( GraphDatabaseService db )
    {
        Node node1;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = db.createNode();
            Node node2 = db.createNode();
            node1.createRelationshipTo( node2, RelationshipType.withName( "relType1" ) );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            node1.delete();
            tx.success();
            // Will throw exception, causing the tx to be rolledback.
            // InvalidRecordException coming, node1 has rels
            assertThrows( ConstraintViolationException.class, tx::close );
        }

        try ( Transaction tx = db.beginTx() )
        {
            node1.setProperty( "foo", "bar" );
            tx.success();
        }
    }
}
