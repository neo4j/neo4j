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
package org.neo4j.graphdb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.index.label.RelationshipTypeScanStore;
import org.neo4j.internal.index.label.TokenScanReader;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.index.label.RelationshipTypeScanStoreUtil.clearRelationshipTypeStoreScan;
import static org.neo4j.internal.index.label.RelationshipTypeScanStoreUtil.enableRelationshipTypeStoreScan;
import static org.neo4j.internal.kernel.api.IndexQuery.fulltextSearch;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@DbmsExtension
class RelationshipTypeScanStoreIT
{
    private static final RelationshipType REL_TYPE = RelationshipType.withName( "REL_TYPE" );
    private static final String PROPERTY = "prop";
    private static final String PROPERTY_VALUE = "value";
    @Inject
    GraphDatabaseService db;
    @Inject
    DbmsController dbmsController;
    @Inject
    FileSystemAbstraction fs;
    @Inject
    DatabaseLayout databaseLayout;

    @BeforeAll
    static void toggleOn()
    {
        enableRelationshipTypeStoreScan();
    }

    @AfterAll
    static void toggleOff()
    {
        clearRelationshipTypeStoreScan();
    }

    @Test
    void shouldBePossibleToResolveDependency()
    {
        getRelationshipTypeScanStore();
        // Should not throw
    }

    @Test
    void shouldSeeAddedRelationship()
    {
        List<Long> expectedIds = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = createRelationship( tx );
            expectedIds.add( relationship.getId() );
            tx.commit();
        }

        assertContainIds( expectedIds );
    }

    @Test
    void shouldNotSeeRemovedRelationship()
    {
        List<Long> expectedIds = new ArrayList<>();
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( TestLabels.LABEL_ONE );
            nodeId = node.getId();
            Relationship relationship1 = node.createRelationshipTo( tx.createNode(), REL_TYPE );
            Relationship relationship2 = createRelationship( tx );
            expectedIds.add( relationship1.getId() );
            expectedIds.add( relationship2.getId() );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
                expectedIds.remove( relationship.getId() );
            }
            tx.commit();
        }

        assertContainIds( expectedIds );
    }

    @Test
    void shouldRebuildIfMissingDuringStartup()
    {
        List<Long> expectedIds = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = createRelationship( tx );
            expectedIds.add( relationship.getId() );
            tx.commit();
        }

        ResourceIterator<File> files = getRelationshipTypeScanStoreFiles();
        dbmsController.restartDbms( builder ->
        {
            files.forEachRemaining( file -> fs.deleteFile( file ) );
            return builder;
        });

        assertContainIds( expectedIds );
    }

    @Test
    void shouldPopulateIndex() throws KernelException
    {
        int numberOfRelationships = 10;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < numberOfRelationships; i++ )
            {
                createRelationship( tx );
            }
            tx.commit();
        }

        String indexName = createFulltextRelationshipIndex();

        assertEquals( numberOfRelationships, countRelationshipsInFulltextIndex( indexName ) );
    }

    @Test
    void shouldBeRecovered()
    {
        List<Long> expectedIds = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = createRelationship( tx );
            expectedIds.add( relationship.getId() );
            tx.commit();
        }

        dbmsController.restartDbms( builder ->
        {
            removeLastCheckpointRecordFromLastLogFile();
            return builder;
        } );

        assertContainIds( expectedIds );
    }

    @Test
    void shouldRecoverIndex() throws KernelException
    {
        int numberOfRelationships = 10;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < numberOfRelationships; i++ )
            {
                createRelationship( tx );
            }
            tx.commit();
        }

        String indexName = createFulltextRelationshipIndex();

        dbmsController.restartDbms( builder ->
        {
            removeLastCheckpointRecordFromLastLogFile();
            return builder;
        } );

        assertEquals( numberOfRelationships, countRelationshipsInFulltextIndex( indexName ) );
    }

    private ResourceIterator<File> getRelationshipTypeScanStoreFiles()
    {
        RelationshipTypeScanStore relationshipTypeScanStore = getRelationshipTypeScanStore();
        return relationshipTypeScanStore.snapshotStoreFiles();
    }

    private Relationship createRelationship( Transaction tx )
    {
        Relationship relationship = tx.createNode().createRelationshipTo( tx.createNode(), REL_TYPE );
        relationship.setProperty( PROPERTY, PROPERTY_VALUE );
        return relationship;
    }

    private void assertContainIds( List<Long> expectedIds )
    {
        int relationshipTypeId = getRelationshipTypeId();
        RelationshipTypeScanStore relationshipTypeScanStore = getRelationshipTypeScanStore();
        TokenScanReader tokenScanReader = relationshipTypeScanStore.newReader();
        PrimitiveLongResourceIterator relationships = tokenScanReader.entitiesWithToken( relationshipTypeId, NULL );
        List<Long> additionalRelationships = new ArrayList<>();
        while ( relationships.hasNext() )
        {
            long next = relationships.next();
            if ( !expectedIds.remove( next ) )
            {
                additionalRelationships.add( next );
            }
        }
        assertThat( expectedIds ).as( "expected ids" ).isEmpty();
        assertThat( additionalRelationships ).as( "additional ids" ).isEmpty();
    }

    private int countRelationshipsInFulltextIndex( String indexName ) throws KernelException
    {
        int relationshipsInIndex;
        try ( Transaction transaction = db.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction)transaction).kernelTransaction();
            IndexDescriptor index = ktx.schemaRead().indexGetForName( indexName );
            relationshipsInIndex = 0;
            try ( RelationshipIndexCursor cursor = ktx.cursors().allocateRelationshipIndexCursor() )
            {
                ktx.dataRead().relationshipIndexSeek( index, cursor, unconstrained(), fulltextSearch( "*" ) );
                while ( cursor.next() )
                {
                    relationshipsInIndex++;
                }
            }
        }
        return relationshipsInIndex;
    }

    private String createFulltextRelationshipIndex()
    {
        String indexName;
        try ( Transaction transaction = db.beginTx() )
        {
            indexName = transaction.schema().indexFor( REL_TYPE ).on( PROPERTY ).withIndexType( IndexType.FULLTEXT ).create().getName();
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            transaction.commit();
        }
        return indexName;
    }

    private int getRelationshipTypeId()
    {
        int relationshipTypeId;
        try ( Transaction tx = db.beginTx() )
        {
            relationshipTypeId = ((InternalTransaction) tx).kernelTransaction().tokenRead().relationshipType( REL_TYPE.name() );
            tx.commit();
        }
        return relationshipTypeId;
    }

    private void removeLastCheckpointRecordFromLastLogFile()
    {
        try
        {
            LogPosition checkpointPosition = null;

            LogFiles logFiles = buildLogFiles();
            LogFile transactionLogFile = logFiles.getLogFile();
            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader();
            LogPosition startPosition = logFiles.extractHeader( logFiles.getHighestLogVersion() ).getStartPosition();
            try ( ReadableLogChannel reader = transactionLogFile.getReader( startPosition ) )
            {
                LogEntry logEntry;
                do
                {
                    logEntry = entryReader.readLogEntry( reader );
                    if ( logEntry instanceof CheckPoint )
                    {
                        checkpointPosition = ((CheckPoint) logEntry).getLogPosition();
                    }
                }
                while ( logEntry != null );
            }
            if ( checkpointPosition != null )
            {
                try ( StoreChannel storeChannel = fs.write( logFiles.getHighestLogFile() ) )
                {
                    storeChannel.truncate( checkpointPosition.getByteOffset() );
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private LogFiles buildLogFiles() throws IOException
    {
        return LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fs ).build();
    }

    private RelationshipTypeScanStore getRelationshipTypeScanStore()
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( RelationshipTypeScanStore.class );
    }
}
