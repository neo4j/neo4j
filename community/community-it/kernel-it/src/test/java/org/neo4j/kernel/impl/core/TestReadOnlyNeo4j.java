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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.StringContainsInOrder.stringContainsInOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;

@EphemeralTestDirectoryExtension
class TestReadOnlyNeo4j
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void testSimple()
    {
        DbRepresentation someData = createSomeData();
        managementService = dbmsReadOnly();
        GraphDatabaseService readGraphDb = managementService.database( DEFAULT_DATABASE_NAME );
        assertEquals( someData, DbRepresentation.of( readGraphDb ) );

        assertThrows( WriteOperationsNotAllowedException.class, () ->
        {
            try ( Transaction tx = readGraphDb.beginTx() )
            {
                tx.createNode();

                tx.commit();
            }
        } );
    }

    @Test
    void databaseNotStartInReadOnlyModeWithMissingIndex()
    {
        createIndex();
        deleteIndexFolder();

        AssertableLogProvider logProvider = new AssertableLogProvider();
        managementService = dbmsReadOnly( logProvider );
        final GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        assertFalse( db.isAvailable( 1L ), "Did not expect db to start" );
        logProvider.internalToStringMessageMatcher().assertContains( stringContainsInOrder(
                "[neo4j] Exception occurred while starting the database. Trying to stop already started components.",
                "Some indexes need to be rebuilt. This is not allowed in read only mode. Please start db in " +
                        "writable mode to rebuild indexes. Indexes needing rebuild: "
        ) );
    }

    @Test
    void testReadOnlyOperationsAndNoTransaction()
    {
        managementService = dbms();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        Transaction tx = db.beginTx();
        Node node1 = tx.createNode();
        Node node2 = tx.createNode();
        Relationship rel = node1.createRelationshipTo( node2, withName( "TEST" ) );
        node1.setProperty( "key1", "value1" );
        rel.setProperty( "key1", "value1" );
        tx.commit();

        // make sure write operations still throw exception
        assertThrows( NotInTransactionException.class, () -> node1.createRelationshipTo( node2, withName( "TEST2" ) ) );
        assertThrows( NotInTransactionException.class, () -> node1.setProperty( "key1", "value2" ) );
        assertThrows( NotInTransactionException.class, () -> rel.removeProperty( "key1" ) );

        try ( Transaction transaction = db.beginTx() )
        {
            assertEquals( node1, transaction.getNodeById( node1.getId() ) );
            assertEquals( node2, transaction.getNodeById( node2.getId() ) );
            assertEquals( rel, transaction.getRelationshipById( rel.getId() ) );

            var loadedNode = transaction.getNodeById( node1.getId() );
            assertThat( loadedNode, hasProperty( "key1" ).withValue( "value1" ) );
            Relationship loadedRel = loadedNode.getSingleRelationship( withName( "TEST" ), Direction.OUTGOING );
            assertEquals( rel, loadedRel );
            assertThat( loadedRel, hasProperty( "key1" ).withValue( "value1" ) );
        }
    }

    private void createIndex()
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                .impermanent()
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( Label.label( "label" ) ).on( "prop" ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
        managementService.shutdown();
    }

    private void deleteIndexFolder()
    {
        File databaseDir = Neo4jLayout.of( testDirectory.homeDir() ).databaseLayout( DEFAULT_DATABASE_NAME ).databaseDirectory();
        fs.deleteRecursively( IndexDirectoryStructure.baseSchemaIndexFolder( databaseDir ) );
    }

    private DbRepresentation createSomeData()
    {
        RelationshipType type = withName( "KNOWS" );
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                .impermanent()
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Node prevNode = tx.createNode();
            for ( int i = 0; i < 100; i++ )
            {
                Node node = tx.createNode();
                Relationship rel = prevNode.createRelationshipTo( node, type );
                node.setProperty( "someKey" + i % 10, i % 15 );
                rel.setProperty( "since", System.currentTimeMillis() );
            }
            tx.commit();
        }
        DbRepresentation result = DbRepresentation.of( db );
        managementService.shutdown();
        return result;
    }

    private DatabaseManagementService dbmsReadOnly( LogProvider logProvider )
    {
        return new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                .impermanent()
                .setConfig( GraphDatabaseSettings.read_only, true )
                .setInternalLogProvider( logProvider )
                .build();
    }

    private DatabaseManagementService dbmsReadOnly()
    {
        return new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                .impermanent()
                .setConfig( GraphDatabaseSettings.read_only, true )
                .build();
    }

    private DatabaseManagementService dbms()
    {
        return new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setFileSystem( fs )
                .impermanent()
                .build();
    }
}
