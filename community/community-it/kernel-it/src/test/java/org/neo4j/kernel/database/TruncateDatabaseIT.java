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
package org.neo4j.kernel.database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.RandomStringUtils.random;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

@DbmsExtension
class TruncateDatabaseIT
{
    @Inject
    private GraphDatabaseAPI databaseAPI;
    private Database database;

    @BeforeEach
    void setUp()
    {
        database = databaseAPI.getDependencyResolver().resolveDependency( Database.class );
    }

    @ParameterizedTest
    @EnumSource( TruncationTypes.class )
    void executeTransactionsOnTrucatedDatabase( DatabaseTruncator truncator )
    {
        createTenNodes();

        truncator.truncate( database );

        createTenNodes();
        assertEquals( 10, countNodes() );
    }

    @ParameterizedTest
    @EnumSource( TruncationTypes.class )
    void truncateDeleteAllNodes( DatabaseTruncator truncator )
    {
        createTenNodes();
        assertEquals( 10, countNodes() );
        truncator.truncate( database );

        assertEquals( 0, countNodes() );
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            assertEquals( 0, databaseAPI.createNode().getId() );
        }
    }

    @ParameterizedTest
    @EnumSource( TruncationTypes.class )
    void truncateDeleteAllRelationships( DatabaseTruncator truncator )
    {
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node nodeA = databaseAPI.createNode();
                Node nodeB = databaseAPI.createNode();
                nodeA.createRelationshipTo( nodeB, withName( "any" ) );
            }
            transaction.commit();
        }
        assertEquals( 10, countRelationships() );
        truncator.truncate( database );

        assertEquals( 0, countRelationships() );
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            Node newA = databaseAPI.createNode();
            Node newB = databaseAPI.createNode();
            assertEquals( 0, newA.createRelationshipTo( newB, withName( "new" ) ).getId() );
        }
    }

    @ParameterizedTest
    @EnumSource( TruncationTypes.class )
    void truncateKeepsRelationshipTypes( DatabaseTruncator truncator )
    {
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node nodeA = databaseAPI.createNode();
                Node nodeB = databaseAPI.createNode();
                nodeA.createRelationshipTo( nodeB, withName( "any" + i ) );
            }
            transaction.commit();
        }
        assertEquals( 10, countRelationshipTypes() );
        truncator.truncate( database );

        assertEquals( 10, countRelationshipTypes() );
    }

    @ParameterizedTest
    @EnumSource( TruncationTypes.class )
    void truncateKeepsLabels( DatabaseTruncator truncator )
    {
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                databaseAPI.createNode( label( "start" + i ));
                databaseAPI.createNode( label( "finish" + i ) );
            }
            transaction.commit();
        }
        assertEquals( 20, countLabels() );
        truncator.truncate( database );

        assertEquals( 20, countLabels() );
    }

    @ParameterizedTest
    @EnumSource( TruncationTypes.class )
    void truncateKeepsPropertyKeys( DatabaseTruncator truncator )
    {
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = databaseAPI.createNode();
                node.setProperty( "a" + i, random( 10 ) );
                node.setProperty( "b" + i, random( 10 ) );
            }
            transaction.commit();
        }
        assertEquals( 20, countPropertyKeys() );
        truncator.truncate( database );

        assertEquals( 20, countPropertyKeys() );
    }

    @ParameterizedTest
    @EnumSource( TruncationTypes.class )
    @Disabled
    void truncateKeepsIndexDefinitions( DatabaseTruncator truncator )
    {
        Label indexLabel = label( "indexLabel" );
        String indexedProperty = "indexedProperty";
        String indexName = "truncatedIndex";
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            databaseAPI.schema().indexFor( indexLabel )
                    .on( indexedProperty )
                    .withName( indexName )
                    .create();
            transaction.commit();
        }
        awaitIndexes();

        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = databaseAPI.createNode( indexLabel );
                node.setProperty( indexedProperty, random( 10 ) );
            }
            transaction.commit();
        }

        truncator.truncate( database );

        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            IndexDefinition indexDefinition = databaseAPI.schema().getIndexByName( indexName );
            assertEquals( indexLabel, indexDefinition.getLabels().iterator().next() );
            assertEquals( indexedProperty, indexDefinition.getPropertyKeys().iterator().next() );
        }
    }

    @ParameterizedTest
    @EnumSource( TruncationTypes.class )
    void truncateDeleteAllTransactionLogs( DatabaseTruncator truncator )
    {
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = databaseAPI.createNode();
                node.setProperty( "a" + i, random( 10 ) );
                node.setProperty( "b" + i, random( 10 ) );
            }
            transaction.commit();
        }
        assertEquals( 20, countPropertyKeys() );
        LogFiles logFiles = getLogFiles();
        long lastEntryId = logFiles.getLogFileInformation().getLastEntryId();
        // at least 10 transactions made it to the logs
        assertThat( lastEntryId, greaterThanOrEqualTo( 10L ) );

        truncator.truncate( database );

        long truncatedLastEntryId = getLogFiles().getLogFileInformation().getLastEntryId();
        assertThat( truncatedLastEntryId, equalTo( 1L ) );
    }

    @ParameterizedTest
    @EnumSource( TruncationTypes.class )
    void truncateDeleteCountsStoreData( DatabaseTruncator truncator )
    {
        Label human = label( "human" );
        RelationshipType relationshipType = withName( "relationship" );
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node nodeA = databaseAPI.createNode( human );
                Node nodeB = databaseAPI.createNode( human );
                nodeA.setProperty( "a" + i, random( 10 ) );
                nodeB.setProperty( "b" + i, random( 10 ) );
                nodeA.createRelationshipTo( nodeB, relationshipType );
            }
            transaction.commit();
        }

        TokenHolders tokenHolders = database.getTokenHolders();
        int labelId = tokenHolders.labelTokens().getIdByName( human.name() );
        int typeId = tokenHolders.relationshipTypeTokens().getIdByName( relationshipType.name() );
        try ( RecordStorageReader reader = getRecordStorageEngine().newReader() )
        {
            assertEquals( 20, reader.countsForNode( labelId ) );
            assertEquals( 10, reader.countsForRelationship( ANY_LABEL, typeId, ANY_LABEL ) );
        }

        truncator.truncate( database );

        try ( RecordStorageReader reader = getRecordStorageEngine().newReader() )
        {
            assertEquals( 0, reader.countsForNode( labelId ) );
            assertEquals( 0, reader.countsForRelationship( ANY_LABEL, typeId, ANY_LABEL ) );
        }
    }

    private RecordStorageEngine getRecordStorageEngine()
    {
        return database.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
    }

    private LogFiles getLogFiles()
    {
        return database.getDependencyResolver().resolveDependency( LogFiles.class );
    }

    private void awaitIndexes()
    {
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            databaseAPI.schema().awaitIndexesOnline( 10, MINUTES );
        }
    }

    private long countNodes()
    {
        try ( Transaction ignored = databaseAPI.beginTx() )
        {
            return count( databaseAPI.getAllNodes() );
        }
    }

    private long countRelationships()
    {
        try ( Transaction ignored = databaseAPI.beginTx() )
        {
            return count( databaseAPI.getAllRelationships() );
        }
    }

    private long countRelationshipTypes()
    {
        try ( Transaction ignored = databaseAPI.beginTx() )
        {
            return count( databaseAPI.getAllRelationshipTypes() );
        }
    }

    private long countLabels()
    {
        try ( Transaction ignored = databaseAPI.beginTx() )
        {
            return count( databaseAPI.getAllLabels() );
        }
    }

    private long countPropertyKeys()
    {
        try ( Transaction ignored = databaseAPI.beginTx() )
        {
            return count( databaseAPI.getAllPropertyKeys() );
        }
    }

    private void createTenNodes()
    {
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                databaseAPI.createNode();
            }
            transaction.commit();
        }
    }

    private interface DatabaseTruncator
    {
        void truncate( Database database );
    }

    private enum TruncationTypes implements DatabaseTruncator
    {
        RUNNING_DATABASE_TRUNCATION
                {
                    @Override
                    public void truncate( Database database )
                    {
                        database.truncate();
                    }
                },
        STOPPED_DATABASE_TRUNCATION
                {
                    @Override
                    public void truncate( Database database )
                    {
                        database.stop();
                        database.truncate();
                        database.start();
                    }
                }
    }

}
