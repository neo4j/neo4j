/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.storage;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.layout.IndexFolderLayout;

public class ShardedIndexStorage extends IndexStorage
{
    private static final int partitions = 1;
    private List<Directory> partitionDirectories = new ArrayList<>();

    public ShardedIndexStorage( DirectoryFactory directoryFactory, FileSystemAbstraction fileSystem,
            File schemaIndexRootFolder, long indexId )
    {
        super( directoryFactory, fileSystem, new IndexFolderLayout( schemaIndexRootFolder, indexId ) );
    }

    @Override
    public void prepareIndexStorage() throws IOException
    {
        Directory directory = openPartitionDirectory( partitions );
        partitionDirectories.add( directory );
        cleanupStorage( directory );
        failureStorage.reserveForIndex();
    }

    @Override
    public void cleanupStorage() throws IOException
    {
        for(int i = 1; i <= partitionDirectories.size(); i++ )
        {
            try
            {
                cleanupStorage( partitionDirectories.get( i - 1 ) );
            }
            catch ( AlreadyClosedException e )
            {
                cleanupStorage( directoryFactory.open( folderLayout.getPartitionFolder( i ) ) );
            }
        }
        close();
        failureStorage.clearForIndex();
    }

    @Override
    public void close() throws IOException
    {
        for ( Directory directory : partitionDirectories )
        {
            try
            {
                directory.close();
            }
            catch ( Throwable e )
            {
                // continue for now
            }
        }
    }

    @Override
    public Directory getDirectory()
    {
        return partitionDirectories.get( partitions - 1 );
    }

    @Override
    public void openDirectory() throws IOException
    {
        Directory directory = openPartitionDirectory( partitions );
        partitionDirectories.add( directory );
    }

    private Directory openPartitionDirectory( int partition ) throws IOException
    {
        return directoryFactory.open( folderLayout.getPartitionFolder( partition ) );
    }

}
