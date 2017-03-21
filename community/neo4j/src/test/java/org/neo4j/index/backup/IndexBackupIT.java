/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.backup;

import org.apache.commons.io.FilenameUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexBackupIT
{
    @Rule
    public EmbeddedDatabaseRule database = new EmbeddedDatabaseRule( getClass() ).startLazily();

    @Test
    public void concurrentIndexSnapshotUseDifferentSnapshots() throws Exception
    {
        Label label = Label.label( "testLabel" );
        prepareDatabase( label );

        CheckPointer checkPointer = resolveDependency( CheckPointer.class );
        IndexingService indexingService = resolveDependency( IndexingService.class );

        forceCheckpoint( checkPointer );
        ResourceIterator<File> firstCheckpointSnapshot = indexingService.snapshotStoreFiles();
        generateData( label );

        forceCheckpoint( checkPointer );
        ResourceIterator<File> secondCheckpointSnapshot = indexingService.snapshotStoreFiles();

        Set<String> firstSnapshotFileNames =  getFileNames( firstCheckpointSnapshot );
        Set<String> secondSnapshotFileNames = getFileNames( secondCheckpointSnapshot );

        for ( String nameInFirstSnapshot : firstSnapshotFileNames )
        {
            assertFalse( "Second snapshot fileset should not have files from first snapshot set." +
                            describeFileSets( firstSnapshotFileNames, secondSnapshotFileNames ),
                    secondSnapshotFileNames.contains( nameInFirstSnapshot ) );
            String path = FilenameUtils.getFullPath( nameInFirstSnapshot );
            assertTrue( "Snapshot should contain files for index in path: " + path + "." +
                            describeFileSets( firstSnapshotFileNames, secondSnapshotFileNames ),
                    secondSnapshotFileNames.stream().anyMatch( name -> name.startsWith( path ) ) );
        }
        firstCheckpointSnapshot.close();
        secondCheckpointSnapshot.close();
    }

    private String describeFileSets(Set<String> firstFileSet, Set<String> secondFileSet)
    {
        return "First snapshot files are: " + firstFileSet + System.lineSeparator() +
                "second snapshot files are: " + secondFileSet;
    }

    private Set<String> getFileNames( ResourceIterator<File> files )
    {
        return files.stream().map( File::getAbsolutePath ).collect( Collectors.toSet() );
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
                database.schema().indexFor( label ).on( "property" + i ).create();
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
        for ( int i = 0; i < 10; i++ )
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
            node.setProperty( i + "property", i );
            transaction.success();
        }
    }

    private <T> T resolveDependency(Class<T> clazz)
    {
        return getDatabaseResolver().resolveDependency( clazz );
    }

    private DependencyResolver getDatabaseResolver()
    {
        return database.getDependencyResolver();
    }
}
