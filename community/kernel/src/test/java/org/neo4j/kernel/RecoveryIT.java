/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.test.AdversarialPageCacheGraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.RelationshipType.withName;

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

        database.shutdown();
        recoveredDatabase.shutdown();
    }

    @Test
    public void shouldRecoverIdsCorrectlyWhenWeCreateAndDeleteANodeInTheSameRecoveryRun() throws IOException
    {
        GraphDatabaseService database = startDatabase( directory.graphDbDir() );
        Label testLabel = Label.label( "testLabel" );
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

    @Test( timeout = 60_000 )
    public void recoveryShouldFixPartiallyAppliedSchemaIndexUpdates()
    {
        Label label = Label.label( "Foo" );
        String property = "Bar";

        // cause failure during 'relationship.delete()' command application
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, true ),
                Command.RelationshipCommand.class );
        adversary.disable();

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File storeDir = directory.graphDbDir();
        GraphDatabaseService db = AdversarialPageCacheGraphDatabaseFactory.create( fs, adversary )
                .newEmbeddedDatabase( storeDir );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
                tx.success();
            }

            long relationshipId = createRelationship( db );

            TransactionFailureException txFailure = null;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( label );
                node.setProperty( property, "B" );
                db.getRelationshipById( relationshipId ).delete(); // this should fail because of the adversary
                tx.success();
                adversary.enable();
            }
            catch ( TransactionFailureException e )
            {
                txFailure = e;
            }
            assertNotNull( txFailure );
            adversary.disable();

            healthOf( db ).healed(); // heal the db so it is possible to inspect the data

            // now we can observe partially committed state: node is in the index and relationship still present
            try ( Transaction tx = db.beginTx() )
            {
                assertNotNull( findNode( db, label, property, "B" ) );
                assertNotNull( db.getRelationshipById( relationshipId ) );
                tx.success();
            }

            healthOf( db ).panic( txFailure.getCause() ); // panic the db again to force recovery on the next startup

            // restart the database, now with regular page cache
            db.shutdown();
            db = startDatabase( storeDir );

            // now we observe correct state: node is in the index and relationship is removed
            try ( Transaction tx = db.beginTx() )
            {
                assertNotNull( findNode( db, label, property, "B" ) );
                assertRelationshipNotExist( db, relationshipId );
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private Node findNodeByLabel( GraphDatabaseService database, Label testLabel )
    {
        try ( ResourceIterator<Node> nodes = database.findNodes( testLabel ) )
        {
            return nodes.next();
        }
    }

    private static Node findNode( GraphDatabaseService db, Label label, String property, String value )
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( label, property, value ) )
        {
            return IteratorUtil.single( nodes );
        }
    }

    private static long createRelationship( GraphDatabaseService db )
    {
        long relationshipId;
        try ( Transaction tx = db.beginTx() )
        {
            Node start = db.createNode( Label.label( System.currentTimeMillis() + "" ) );
            Node end = db.createNode( Label.label( System.currentTimeMillis() + "" ) );
            relationshipId = start.createRelationshipTo( end, withName( "KNOWS" ) ).getId();
            tx.success();
        }
        return relationshipId;
    }

    private static void assertRelationshipNotExist( GraphDatabaseService db, long id )
    {
        try
        {
            db.getRelationshipById( id );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NotFoundException.class ) );
        }
    }

    private static DatabaseHealth healthOf( GraphDatabaseService db )
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        return resolver.resolveDependency( DatabaseHealth.class );
    }

    private String createLongString()
    {
        String[] strings = new String[(int) ByteUnit.kibiBytes( 2 )];
        Arrays.fill( strings, "a" );
        return Arrays.toString( strings );
    }

    private GraphDatabaseService startDatabase( File storeDir )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
    }

    private File copyTransactionLogs() throws IOException
    {
        File restoreDbStoreDir = this.directory.directory( "restore-db" );
        LogFiles.move( new DefaultFileSystemAbstraction(), this.directory.graphDbDir(), restoreDbStoreDir );
        return restoreDbStoreDir;
    }
}
