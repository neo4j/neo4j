/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.core;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

public class TestReadOnlyNeo4j
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void testSimple()
    {
        File databaseDir = testDirectory.databaseDir();
        FileSystemAbstraction fs = testDirectory.getFileSystem();
        DbRepresentation someData = createSomeData( databaseDir, fs );
        GraphDatabaseService readGraphDb = new TestGraphDatabaseFactory()
                .setFileSystem( fs )
                .newEmbeddedDatabaseBuilder( databaseDir )
                .setConfig( GraphDatabaseSettings.read_only, Settings.TRUE )
                .newGraphDatabase();
        assertEquals( someData, DbRepresentation.of( readGraphDb ) );

        try ( Transaction tx = readGraphDb.beginTx() )
        {
            readGraphDb.createNode();
            tx.success();
            fail( "Should have failed" );
        }
        catch ( WriteOperationsNotAllowedException e )
        {
            // good
        }
        readGraphDb.shutdown();
    }

    @Test
    public void databaseNotStartInReadOnlyModeWithMissingIndex() throws IOException
    {
        File databaseDir = testDirectory.databaseDir();
        FileSystemAbstraction fs = testDirectory.getFileSystem();
        createIndex( databaseDir, fs );
        deleteIndexFolder( databaseDir, fs );
        GraphDatabaseService readGraphDb = null;
        try
        {
            readGraphDb = new TestGraphDatabaseFactory()
                    .setFileSystem( fs )
                    .newImpermanentDatabaseBuilder( databaseDir )
                    .setConfig( GraphDatabaseSettings.read_only, Settings.TRUE )
                    .newGraphDatabase();
            fail( "Should have failed" );
        }
        catch ( RuntimeException e )
        {
            Throwable rootCause = Exceptions.rootCause( e );
            assertTrue( rootCause instanceof IllegalStateException );
            assertTrue( rootCause.getMessage().contains(
                    "Some indexes need to be rebuilt. This is not allowed in read only mode. Please start db in writable mode to rebuild indexes. Indexes " +
                            "needing rebuild:" ) );
        }
        finally
        {
            if ( readGraphDb != null )
            {
                readGraphDb.shutdown();
            }
        }
    }

    @Test
    public void testReadOnlyOperationsAndNoTransaction()
    {
        FileSystemAbstraction fs = testDirectory.getFileSystem();
        File databaseDir = testDirectory.databaseDir();
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabase( databaseDir );

        Transaction tx = db.beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        Relationship rel = node1.createRelationshipTo( node2, withName( "TEST" ) );
        node1.setProperty( "key1", "value1" );
        rel.setProperty( "key1", "value1" );
        tx.success();
        tx.close();

        // make sure write operations still throw exception
        try
        {
            db.createNode();
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            node1.createRelationshipTo( node2, withName( "TEST2" ) );
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            node1.setProperty( "key1", "value2" );
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }

        try
        {
            rel.removeProperty( "key1" );
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }

        Transaction transaction = db.beginTx();
        assertEquals( node1, db.getNodeById( node1.getId() ) );
        assertEquals( node2, db.getNodeById( node2.getId() ) );
        assertEquals( rel, db.getRelationshipById( rel.getId() ) );

        assertThat( node1, inTx( db, hasProperty( "key1" ).withValue( "value1" ) ) );
        Relationship loadedRel = node1.getSingleRelationship( withName( "TEST" ), Direction.OUTGOING );
        assertEquals( rel, loadedRel );
        assertThat(loadedRel, inTx(db, hasProperty( "key1" ).withValue( "value1" )));
        transaction.close();
        db.shutdown();
    }

    private void deleteIndexFolder( File databaseDir, FileSystemAbstraction fs ) throws IOException
    {
        fs.deleteRecursively( IndexDirectoryStructure.baseSchemaIndexFolder( databaseDir ) );
    }

    private void createIndex( File databaseDir, FileSystemAbstraction fs )
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( fs )
                .newEmbeddedDatabase( databaseDir );
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( Label.label( "label" ) ).on( "prop" ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
        db.shutdown();
    }

    private DbRepresentation createSomeData( File databaseDir, FileSystemAbstraction fs )
    {
        RelationshipType type = withName( "KNOWS" );
        GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( fs )
                .newImpermanentDatabase( databaseDir );
        try ( Transaction tx = db.beginTx() )
        {
            Node prevNode = db.createNode();
            for ( int i = 0; i < 100; i++ )
            {
                Node node = db.createNode();
                Relationship rel = prevNode.createRelationshipTo( node, type );
                node.setProperty( "someKey" + i % 10, i % 15 );
                rel.setProperty( "since", System.currentTimeMillis() );
            }
            tx.success();
        }
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }
}
