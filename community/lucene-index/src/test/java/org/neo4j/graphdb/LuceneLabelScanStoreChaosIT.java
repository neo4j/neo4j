/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.graphdb;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.kernel.api.impl.labelscan.LuceneLabelScanIndexBuilder;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

/**
 * Tests functionality around missing or corrupted lucene label scan store index, and that
 * the database should repair (i.e. rebuild) that automatically and just work.
 */
public class LuceneLabelScanStoreChaosIT
{
    @Rule
    public final DatabaseRule dbRule = new EmbeddedDatabaseRule( getClass() );
    private final Random random = new Random();

    @Test
    public void shouldRebuildDeletedLabelScanStoreOnStartup() throws Exception
    {
        // GIVEN
        Node node1 = createLabeledNode( Labels.First );
        Node node2 = createLabeledNode( Labels.First );
        Node node3 = createLabeledNode( Labels.First );
        deleteNode( node2 ); // just to create a hole in the store

        // WHEN
        dbRule.restartDatabase( deleteTheLabelScanStoreIndex() );

        // THEN
        assertEquals( asSet( node1, node3 ), getAllNodesWithLabel( Labels.First ) );
    }

    @Test
    public void rebuildCorruptedLabelScanStoreToStartup() throws Exception
    {
        Node node = createLabeledNode( Labels.First );

        dbRule.restartDatabase( corruptTheLabelScanStoreIndex() );

        assertEquals( asSet( node ), getAllNodesWithLabel( Labels.First ) );
    }

    private DatabaseRule.RestartAction corruptTheLabelScanStoreIndex()
    {
        return ( fs, storeDirectory ) -> {
            try
            {
                int filesCorrupted = 0;
                List<File> partitionDirs = labelScanStoreIndexDirectories( storeDirectory );
                for ( File partitionDir : partitionDirs )
                {
                    for ( File file : partitionDir.listFiles() )
                    {
                        scrambleFile( file );
                        filesCorrupted++;
                    }
                }
                assertTrue( "No files found to corrupt", filesCorrupted > 0 );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private DatabaseRule.RestartAction deleteTheLabelScanStoreIndex()
    {
        return ( fs, storeDirectory ) -> {
            try
            {
                List<File> partitionDirs = labelScanStoreIndexDirectories( storeDirectory );
                for ( File dir : partitionDirs )
                {
                    assertTrue( "We seem to want to delete the wrong directory here", dir.exists() );
                    assertTrue( "No index files to delete", dir.listFiles().length > 0 );
                    deleteRecursively( dir );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private List<File> labelScanStoreIndexDirectories( File storeDirectory )
    {
        File rootDir = new File( new File( new File( new File( storeDirectory, "schema" ), "label" ), "lucene" ),
                LuceneLabelScanIndexBuilder.DEFAULT_INDEX_IDENTIFIER );

        File[] partitionDirs = rootDir.listFiles( File::isDirectory );
        return (partitionDirs == null) ? Collections.emptyList() : Stream.of( partitionDirs ).collect( toList() );
    }

    private Node createLabeledNode( Label... labels )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseAPI().beginTx() )
        {
            Node node = dbRule.getGraphDatabaseAPI().createNode( labels );
            tx.success();
            return node;
        }
    }

    private Set<Node> getAllNodesWithLabel( Label label )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseAPI().beginTx() )
        {
            return asSet( dbRule.getGraphDatabaseAPI().findNodes( label ) );
        }
    }

    private void deleteNode( Node node )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseAPI().beginTx() )
        {
            node.delete();
            tx.success();
        }
    }

    private void scrambleFile( File file ) throws IOException
    {
        try ( RandomAccessFile fileAccess = new RandomAccessFile( file, "rw" );
              FileChannel channel = fileAccess.getChannel() )
        {
            // The files will be small, so OK to allocate a buffer for the full size
            byte[] bytes = new byte[(int) channel.size()];
            putRandomBytes( bytes );
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            channel.position( 0 );
            channel.write( buffer );
        }
    }

    private void putRandomBytes( byte[] bytes )
    {
        for ( int i = 0; i < bytes.length; i++ )
        {
            bytes[i] = (byte) random.nextInt();
        }
    }

    private enum Labels implements Label
    {
        First, Second, Third
    }
}
