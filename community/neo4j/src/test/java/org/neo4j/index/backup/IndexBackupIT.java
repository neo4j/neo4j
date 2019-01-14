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
package org.neo4j.index.backup;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.index.IndexFileNames;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static java.lang.String.format;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class IndexBackupIT
{
    private static final String PROPERTY_PREFIX = "property";
    private static final int NUMBER_OF_INDEXES = 10;

    @Rule
    public RandomRule randomRule = new RandomRule();
    @Rule
    public EmbeddedDatabaseRule database = new EmbeddedDatabaseRule();
    private CheckPointer checkPointer;
    private IndexingService indexingService;
    private FileSystemAbstraction fileSystem;

    @Before
    public void setUp()
    {
        checkPointer = resolveDependency( CheckPointer.class );
        indexingService = resolveDependency( IndexingService.class );
        fileSystem = resolveDependency( FileSystemAbstraction.class );
    }

    @Test
    public void concurrentIndexSnapshotUseDifferentSnapshots() throws Exception
    {
        Label label = Label.label( "testLabel" );
        prepareDatabase( label );

        forceCheckpoint( checkPointer );
        ResourceIterator<File> firstCheckpointSnapshot = indexingService.snapshotIndexFiles();
        generateData( label );
        removeOldNodes( LongStream.range( 1, 20 )  );
        updateOldNodes( LongStream.range( 30, 40 ) );

        forceCheckpoint( checkPointer );
        ResourceIterator<File> secondCheckpointSnapshot = indexingService.snapshotIndexFiles();

        generateData( label );
        removeOldNodes( LongStream.range( 50, 60 )  );
        updateOldNodes( LongStream.range( 70, 80 ) );

        forceCheckpoint( checkPointer );
        ResourceIterator<File> thirdCheckpointSnapshot = indexingService.snapshotIndexFiles();

        Set<String> firstSnapshotFileNames =  getFileNames( firstCheckpointSnapshot );
        Set<String> secondSnapshotFileNames = getFileNames( secondCheckpointSnapshot );
        Set<String> thirdSnapshotFileNames = getFileNames( thirdCheckpointSnapshot );

        compareSnapshotFiles( firstSnapshotFileNames, secondSnapshotFileNames, fileSystem );
        compareSnapshotFiles( secondSnapshotFileNames, thirdSnapshotFileNames, fileSystem);
        compareSnapshotFiles( thirdSnapshotFileNames, firstSnapshotFileNames, fileSystem);

        firstCheckpointSnapshot.close();
        secondCheckpointSnapshot.close();
        thirdCheckpointSnapshot.close();

    }

    @Test
    public void snapshotFilesDeletedWhenSnapshotReleased() throws IOException
    {
        Label label = Label.label( "testLabel" );
        prepareDatabase( label );

        ResourceIterator<File> firstCheckpointSnapshot = indexingService.snapshotIndexFiles();
        generateData( label );
        ResourceIterator<File> secondCheckpointSnapshot = indexingService.snapshotIndexFiles();
        generateData( label );
        ResourceIterator<File> thirdCheckpointSnapshot = indexingService.snapshotIndexFiles();

        Set<String> firstSnapshotFileNames =  getFileNames( firstCheckpointSnapshot );
        Set<String> secondSnapshotFileNames = getFileNames( secondCheckpointSnapshot );
        Set<String> thirdSnapshotFileNames = getFileNames( thirdCheckpointSnapshot );

        generateData( label );
        forceCheckpoint( checkPointer );

        assertTrue( firstSnapshotFileNames.stream().map( File::new ).allMatch( fileSystem::fileExists ) );
        assertTrue( secondSnapshotFileNames.stream().map( File::new ).allMatch( fileSystem::fileExists ) );
        assertTrue( thirdSnapshotFileNames.stream().map( File::new ).allMatch( fileSystem::fileExists ) );

        firstCheckpointSnapshot.close();
        secondCheckpointSnapshot.close();
        thirdCheckpointSnapshot.close();

        generateData( label );
        forceCheckpoint( checkPointer );

        assertFalse( firstSnapshotFileNames.stream().map( File::new ).anyMatch( fileSystem::fileExists ) );
        assertFalse( secondSnapshotFileNames.stream().map( File::new ).anyMatch( fileSystem::fileExists ) );
        assertFalse( thirdSnapshotFileNames.stream().map( File::new ).anyMatch( fileSystem::fileExists ) );
    }

    private void compareSnapshotFiles( Set<String> firstSnapshotFileNames, Set<String> secondSnapshotFileNames,
            FileSystemAbstraction fileSystem )
    {
        assertThat( format( "Should have %d modified index segment files. Snapshot segment files are: %s",
                        NUMBER_OF_INDEXES, firstSnapshotFileNames ), firstSnapshotFileNames,
                hasSize( NUMBER_OF_INDEXES ) );
        for ( String fileName : firstSnapshotFileNames )
        {
            assertFalse( "Snapshot segments fileset should not have files from another snapshot set." +
                            describeFileSets( firstSnapshotFileNames, secondSnapshotFileNames ),
                    secondSnapshotFileNames.contains( fileName ) );
            String path = FilenameUtils.getFullPath( fileName );
            assertTrue( "Snapshot should contain files for index in path: " + path + "." +
                            describeFileSets( firstSnapshotFileNames, secondSnapshotFileNames ),
                    secondSnapshotFileNames.stream().anyMatch( name -> name.startsWith( path ) ) );
            assertTrue( format( "Snapshot segment file '%s' should exist.", fileName ),
                    fileSystem.fileExists( new File( fileName ) ) );
        }
    }

    private void removeOldNodes( LongStream idRange )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            idRange.mapToObj( id -> database.getNodeById( id ) ).forEach( Node::delete );
            transaction.success();
        }
    }

    private void updateOldNodes( LongStream idRange )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            List<Node> nodes = idRange.mapToObj( id -> database.getNodeById( id ) ).collect( Collectors.toList() );
            for ( int i = 0; i < NUMBER_OF_INDEXES; i++ )
            {
                String propertyName = PROPERTY_PREFIX + i;
                nodes.forEach( node -> node.setProperty( propertyName, randomRule.nextLong() ) );
            }
            transaction.success();
        }
    }

    private String describeFileSets( Set<String> firstFileSet, Set<String> secondFileSet )
    {
        return "First snapshot files are: " + firstFileSet + System.lineSeparator() +
                "second snapshot files are: " + secondFileSet;
    }

    private Set<String> getFileNames( ResourceIterator<File> files )
    {
        return files.stream().map( File::getAbsolutePath )
                .filter( this::segmentsFilePredicate )
                .collect( Collectors.toSet() );
    }

    private void forceCheckpoint( CheckPointer checkPointer ) throws IOException
    {
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "testForcedCheckpoint" ) );
    }

    private void prepareDatabase( Label label )
    {
        generateData( label );

        try ( Transaction transaction = database.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                database.schema().indexFor( label ).on( PROPERTY_PREFIX + i ).create();
            }
            transaction.success();
        }

        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }

    private void generateData( Label label )
    {
        for ( int i = 0; i < 100; i++ )
        {
            testNodeCreationTransaction( label, i );
        }
    }

    private void testNodeCreationTransaction( Label label, int i )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "property" + i, i );
            transaction.success();
        }
    }

    private <T> T resolveDependency( Class<T> clazz )
    {
        return getDatabaseResolver().resolveDependency( clazz );
    }

    private DependencyResolver getDatabaseResolver()
    {
        return database.getDependencyResolver();
    }

    private boolean segmentsFilePredicate( String fileName )
    {
        return FilenameUtils.getName( fileName ).startsWith( IndexFileNames.SEGMENTS );
    }
}
