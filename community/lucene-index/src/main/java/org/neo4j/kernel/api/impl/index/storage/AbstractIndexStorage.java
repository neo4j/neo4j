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
package org.neo4j.kernel.api.impl.index.storage;


import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.layout.FolderLayout;

public abstract class AbstractIndexStorage
{
    protected final DirectoryFactory directoryFactory;
    protected final FileSystemAbstraction fileSystem;
    protected final FolderLayout folderLayout;
    protected final FailureStorage failureStorage;

    public AbstractIndexStorage( DirectoryFactory directoryFactory, FileSystemAbstraction fileSystem,
            FolderLayout folderLayout )
    {
        this.fileSystem = fileSystem;
        this.folderLayout = folderLayout;
        this.directoryFactory = directoryFactory;
        this.failureStorage = new FailureStorage( fileSystem, folderLayout );
    }

    public Directory openDirectory( File folder ) throws IOException
    {
        return directoryFactory.open( folder );
    }

    public File getPartitionFolder( int partition )
    {
        return folderLayout.getPartitionFolder( partition );
    }

    public File getIndexFolder()
    {
        return folderLayout.getIndexFolder();
    }

    public void reserveIndexFailureStorage() throws IOException
    {
        failureStorage.reserveForIndex();
    }

    public void storeIndexFailure( String failure ) throws IOException
    {
        failureStorage.storeIndexFailure( failure );
    }

    public String getStoredIndexFailure()
    {
        return failureStorage.loadIndexFailure();
    }

    public void prepareFolder( File folder ) throws IOException
    {
        cleanupFolder( folder );
        fileSystem.mkdirs( folder );
    }

    public void cleanupFolder( File folder ) throws IOException
    {
        fileSystem.deleteRecursively( folder );
    }
}
