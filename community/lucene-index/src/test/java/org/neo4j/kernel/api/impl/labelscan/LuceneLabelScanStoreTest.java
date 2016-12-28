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
package org.neo4j.kernel.api.impl.labelscan;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Random;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;

@RunWith( Parameterized.class )
public class LuceneLabelScanStoreTest extends LabelScanStoreTest
{
    @Parameterized.Parameter
    public BitmapDocumentFormat documentFormat;
    private final DirectoryFactory inMemoryDirectoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private PartitionedIndexStorage indexStorage;

    @Parameterized.Parameters( name = "{0}" )
    public static List<BitmapDocumentFormat> parameterizedWithStrategies()
    {
        return asList( BitmapDocumentFormat._32, BitmapDocumentFormat._64 );
    }

    @Override
    protected LabelScanStore createLabelScanStore( FileSystemAbstraction fs, File rootFolder,
            List<NodeLabelUpdate> existingData, boolean usePersistentStore, boolean readOnly,
            LabelScanStore.Monitor monitor )
    {
        DirectoryFactory directoryFactory = usePersistentStore ? DirectoryFactory.PERSISTENT : inMemoryDirectoryFactory;

        indexStorage = new PartitionedIndexStorage( directoryFactory, fs, rootFolder,
                        LuceneLabelScanIndexBuilder.DEFAULT_INDEX_IDENTIFIER, false );
        Config config = Config.defaults().with( MapUtil.stringMap(
                GraphDatabaseSettings.read_only.name(), String.valueOf( readOnly ) ) );

        LabelScanIndex index = LuceneLabelScanIndexBuilder.create()
                .withDirectoryFactory( directoryFactory )
                .withIndexStorage( indexStorage )
                .withOperationalMode( OperationalMode.single )
                .withConfig( config )
                .withDocumentFormat( documentFormat )
                .build();

        return new LuceneLabelScanStore( index, asStream( existingData ), NullLogProvider.getInstance(), monitor );
    }

    @Override
    protected void corruptIndex() throws IOException
    {
        List<File> indexPartitions = indexStorage.listFolders();
        for ( File partition : indexPartitions )
        {
            File[] files = partition.listFiles();
            if ( files != null )
            {
                for ( File indexFile : files )
                {
                    scrambleFile( indexFile );
                }
            }
        }
    }
    private void scrambleFile( File file ) throws IOException
    {
        try ( RandomAccessFile fileAccess = new RandomAccessFile( file, "rw" );
              FileChannel channel = fileAccess.getChannel() )
        {
            // The files will be small, so OK to allocate a buffer for the full size
            byte[] bytes = new byte[(int) channel.size()];
            putRandomBytes( random.random(), bytes );
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            channel.position( 0 );
            channel.write( buffer );
        }
    }

    private void putRandomBytes( Random random, byte[] bytes )
    {
        for ( int i = 0; i < bytes.length; i++ )
        {
            bytes[i] = (byte) random.nextInt();
        }
    }
}
