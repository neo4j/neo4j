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
package org.neo4j.graphdb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Predicates;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.DatabaseRule.RestartAction;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.Exceptions.peel;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

/**
 * Tests functionality around missing or corrupted lucene label scan store index, and that
 * the database should repair (i.e. rebuild) that automatically and just work.
 */
public class LuceneLabelScanStoreChaosIT
{
    @Test
    public void shouldRebuildDeletedLabelScanStoreOnStartup() throws Exception
    {
        // GIVEN
        Node node1 = createLabeledNode( Labels.First );
        Node node2 = createLabeledNode( Labels.First );
        Node node3 = createLabeledNode( Labels.First );
        deleteNode( node2 ); // just to create a hole in the store

        // WHEN
        // TODO how do we make sure it was deleted and then fully rebuilt? I mean if we somehow deleted
        // the wrong directory here then it would also work, right?
        dbRule.restartDatabase( deleteTheLabelScanStoreIndex() );

        // THEN
        assertEquals(
                asSet( node1, node3 ),
                getAllNodesWithLabel( Labels.First ) );
    }

    @Test
    public void shouldPreventCorruptedLabelScanStoreToStartup() throws Exception
    {
        // GIVEN
        createLabeledNode( Labels.First );

        // WHEN
        // TODO how do we make sure it was deleted and then fully rebuilt? I mean if we somehow deleted
        // the wrong directory here then it would also work, right?
        try
        {
            dbRule.restartDatabase( corruptTheLabelScanStoreIndex() );
            fail( "Shouldn't be able to start up" );
        }
        catch ( RuntimeException e )
        {
            // THEN
            @SuppressWarnings( "unchecked" )
            Throwable ioe = peel( e, Predicates.<Throwable>instanceOf( RuntimeException.class ) );
            assertThat( ioe.getMessage(), containsString( "Label scan store could not be read" ) );
        }
    }

    private RestartAction corruptTheLabelScanStoreIndex()
    {
        return new RestartAction()
        {
            @Override
            public void run( FileSystemAbstraction fs, File storeDirectory )
            {
                try
                {
                    int filesCorrupted = 0;
                    for ( File file : labelScanStoreIndexDirectory( storeDirectory ).listFiles() )
                    {
                        scrambleFile( file );
                        filesCorrupted++;
                    }
                    assertTrue( "No files found to corrupt", filesCorrupted > 0 );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }

    private RestartAction deleteTheLabelScanStoreIndex()
    {
        return new RestartAction()
        {
            @Override
            public void run( FileSystemAbstraction fs, File storeDirectory )
            {
                try
                {
                    File directory = labelScanStoreIndexDirectory( storeDirectory );
                    assertTrue( "We seem to want to delete the wrong directory here", directory.exists() );
                    assertTrue( "No index files to delete", directory.listFiles().length > 0 );
                    deleteRecursively( directory );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }

    private File labelScanStoreIndexDirectory( File storeDirectory )
    {
        File directory = new File( new File( new File( storeDirectory, "schema" ), "label" ), "lucene" );
        return directory;
    }

    private Node createLabeledNode( Label... labels )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseService().beginTx() )
        {
            Node node = dbRule.getGraphDatabaseService().createNode( labels );
            tx.success();
            return node;
        }
    }

    private Set<Node> getAllNodesWithLabel( Label label )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseService().beginTx() )
        {
            return asSet( dbRule.getGraphDatabaseService().findNodes( label ) );
        }
    }

    private void deleteNode( Node node )
    {
        try ( Transaction tx = dbRule.getGraphDatabaseService().beginTx() )
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

    private static enum Labels implements Label
    {
        First,
        Second,
        Third;
    }

    public final @Rule DatabaseRule dbRule = new EmbeddedDatabaseRule( getClass() );
    private final Random random = new Random();
}
