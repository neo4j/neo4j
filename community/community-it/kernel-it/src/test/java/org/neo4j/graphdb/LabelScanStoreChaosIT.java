/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Random;
import java.util.Set;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.fs.FileUtils.writeAll;

/**
 * Tests functionality around missing or corrupted label scan store index, and that
 * the database should repair (i.e. rebuild) that automatically and just work.
 */
@DbmsExtension
@ExtendWith( RandomExtension.class )
public class LabelScanStoreChaosIT
{
    @Inject
    private RandomRule random;
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DbmsController controller;

    @Test
    void shouldRebuildDeletedLabelScanStoreOnStartup()
    {
        Node node1 = createLabeledNode( Labels.First );
        Node node2 = createLabeledNode( Labels.First );
        Node node3 = createLabeledNode( Labels.First );
        deleteNode( node2 ); // just to create a hole in the store

        controller.restartDbms( builder ->
        {
            try
            {
                fs.deleteFile( storeFile( db.databaseLayout() ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
            return builder;
        } );

        assertEquals( asSet( node1, node3 ), getAllNodesWithLabel( Labels.First ) );
    }

    @Test
    void rebuildCorruptedLabelScanStoreToStartup()
    {
        Node node = createLabeledNode( Labels.First );

        controller.restartDbms( builder ->
        {
            scrambleFile( storeFile( db.databaseLayout() ) );
            return builder;
        } );

        assertEquals( asSet( node ), getAllNodesWithLabel( Labels.First ) );
    }

    private static java.nio.file.Path storeFile( DatabaseLayout databaseLayout )
    {
        return databaseLayout.labelScanStore();
    }

    private Node createLabeledNode( Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( labels );
            tx.commit();
            return node;
        }
    }

    private Set<Node> getAllNodesWithLabel( Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            return asSet( tx.findNodes( label ) );
        }
    }

    private void scrambleFile( java.nio.file.Path path )
    {
        scrambleFile( random.random(), path );
    }

    private void deleteNode( Node node )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).delete();
            tx.commit();
        }
    }

    private enum Labels implements Label
    {
        First, Second, Third
    }

    public static void scrambleFile( Random random, Path file )
    {
        try ( FileChannel channel = FileChannel.open( file, READ, WRITE ) )
        {
            // The files will be small, so OK to allocate a buffer for the full size
            byte[] bytes = new byte[(int) channel.size()];
            putRandomBytes( random, bytes );
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            channel.position( 0 );
            writeAll( channel, buffer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static void putRandomBytes( Random random, byte[] bytes )
    {
        for ( int i = 0; i < bytes.length; i++ )
        {
            bytes[i] = (byte) random.nextInt();
        }
    }
}
