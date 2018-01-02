/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RecoveryIT
{
    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void idGeneratorsRebuildAfterRecovery() throws IOException
    {
        GraphDatabaseService database = startDatabase( directory.graphDbDir() );
        int numberOfNodes = 10;
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int nodeIndex = 0; nodeIndex < numberOfNodes; nodeIndex++ )
            {
                database.createNode();
            }
            transaction.success();
        }

        // copying only transaction log simulate non clean shutdown db that should be able to recover just from logs
        File restoreDbStoreDir = copyTransactionLogs();

        GraphDatabaseService recoveredDatabase = startDatabase( restoreDbStoreDir );
        NeoStores neoStore =
                ((GraphDatabaseAPI) recoveredDatabase).getDependencyResolver().resolveDependency( NeoStores.class );
        assertEquals( numberOfNodes, neoStore.getNodeStore().getHighId() );
        // Make sure id generator has been rebuilt so this doesn't throw null pointer exception
        assertTrue( neoStore.getNodeStore().nextId() > 0 );

        database.shutdown();
        recoveredDatabase.shutdown();
    }

    @Test
    public void shouldRecoverIdsCorrectlyWhenWeCreateAndDeleteANodeInTheSameRecoveryRun() throws IOException
    {
        GraphDatabaseService database = startDatabase( directory.graphDbDir() );
        Label testLabel = DynamicLabel.label( "testLabel" );
        final String propertyToDelete = "propertyToDelete";
        final String validPropertyName = "validProperty";

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.addLabel( testLabel );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = findNodeByLabel( database, testLabel );
            node.setProperty( propertyToDelete, createLongString() );
            node.setProperty( validPropertyName, createLongString() );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = findNodeByLabel( database, testLabel );
            node.removeProperty( propertyToDelete );
            transaction.success();
        }

        // copying only transaction log simulate non clean shutdown db that should be able to recover just from logs
        File restoreDbStoreDir = copyTransactionLogs();

        // database should be restored and node should have expected properties
        GraphDatabaseService recoveredDatabase = startDatabase( restoreDbStoreDir );
        try ( Transaction ignored = recoveredDatabase.beginTx() )
        {
            Node node = findNodeByLabel( recoveredDatabase, testLabel );
            assertFalse( node.hasProperty( propertyToDelete ) );
            assertTrue( node.hasProperty( validPropertyName ) );
        }

        database.shutdown();
        recoveredDatabase.shutdown();
    }

    private Node findNodeByLabel( GraphDatabaseService database, Label testLabel )
    {
        try ( ResourceIterator<Node> nodes = database.findNodes( testLabel ) )
        {
            return nodes.next();
        }
    }

    private String createLongString()
    {
        String[] strings = new String[(int) ByteUnit.kibiBytes( 2 )];
        Arrays.fill( strings, "a" );
        return Arrays.toString( strings );
    }

    private GraphDatabaseService startDatabase( File storeDir )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() );
    }

    private File copyTransactionLogs() throws IOException
    {
        File restoreDbStoreDir = this.directory.directory( "restore-db" );
        LogFiles.move( new DefaultFileSystemAbstraction(), this.directory.graphDbDir(), restoreDbStoreDir );
        return restoreDbStoreDir;
    }
}
