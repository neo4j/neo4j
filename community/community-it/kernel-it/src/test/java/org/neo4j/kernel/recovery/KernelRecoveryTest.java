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
package org.neo4j.kernel.recovery;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@EphemeralTestDirectoryExtension
class KernelRecoveryTest
{
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;

    @AfterEach
    void cleanUp()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldHandleWritesProperlyAfterRecovery() throws Exception
    {
        // Given
        GraphDatabaseService db = newDB( fileSystem, "main" );
        long node1 = createNode( db, "k", "v1" );

        // And given the power goes out
        List<TransactionRepresentation> transactions = new ArrayList<>();
        long node2;
        try ( EphemeralFileSystemAbstraction crashedFs = fileSystem.snapshot() )
        {
            managementService.shutdown();
            db = newDB( crashedFs, "main" );
            node2 = createNode( db, "k", "v2" );
            extractTransactions( (GraphDatabaseAPI) db, transactions );
            managementService.shutdown();
        }

        // Then both those nodes should be there, i.e. they are properly there in the log
        GraphDatabaseService rebuilt = newDB( fileSystem, "rebuilt" );
        applyTransactions( transactions, (GraphDatabaseAPI) rebuilt );
        try ( Transaction tx = rebuilt.beginTx() )
        {
            assertEquals( "v1", tx.getNodeById( node1 ).getProperty( "k" ) );
            assertEquals( "v2", tx.getNodeById( node2 ).getProperty( "k" ) );
            tx.commit();
        }
    }

    private static void applyTransactions( List<TransactionRepresentation> transactions, GraphDatabaseAPI rebuilt ) throws TransactionFailureException
    {
        TransactionCommitProcess commitProcess = rebuilt.getDependencyResolver().resolveDependency( TransactionCommitProcess.class );
        for ( TransactionRepresentation transaction : transactions )
        {
            commitProcess.commit( new TransactionToApply( transaction ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
        }
    }

    private static void extractTransactions( GraphDatabaseAPI db, List<TransactionRepresentation> transactions ) throws java.io.IOException
    {
        LogicalTransactionStore txStore = db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
        try ( TransactionCursor cursor = txStore.getTransactions( TransactionIdStore.BASE_TX_ID + 1 ) )
        {
            cursor.forAll( tx -> transactions.add( tx.getTransactionRepresentation() ) );
        }
    }

    private GraphDatabaseService newDB( FileSystemAbstraction fs, String name )
    {
        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.directory( name ) )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                .impermanent()
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static long createNode( GraphDatabaseService db, String key, Object value )
    {
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            node.setProperty( key, value );
            nodeId = node.getId();
            tx.commit();
        }
        return nodeId;
    }
}
