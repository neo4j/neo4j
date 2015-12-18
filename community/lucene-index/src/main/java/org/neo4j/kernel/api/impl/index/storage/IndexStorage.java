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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.layout.FolderLayout;

import static org.neo4j.io.fs.FileUtils.windowsSafeIOOperation;

public class IndexStorage
{
    protected final DirectoryFactory directoryFactory;
    protected final FolderLayout folderLayout;
    protected final FailureStorage failureStorage;

    private Directory directory;

    public IndexStorage( DirectoryFactory directoryFactory, FileSystemAbstraction fileSystem,
            FolderLayout folderLayout )
    {
        this.folderLayout = folderLayout;
        this.directoryFactory = directoryFactory;
        this.failureStorage = new FailureStorage( fileSystem, folderLayout );
    }

    public void close() throws IOException
    {
        if ( directory != null )
        {
            directory.close();
        }
    }

    public Directory getDirectory()
    {
        return directory;
    }

    public void prepareIndexStorage() throws IOException
    {
        openDirectory();
        cleanupStorage( directory );
        failureStorage.reserveForIndex();
    }

    public void storeIndexFailure( String failure ) throws IOException
    {
        failureStorage.storeIndexFailure( failure );
    }

    public File getIndexFolder()
    {
        return folderLayout.getIndexFolder();
    }

    public String getStoredIndexFailure()
    {
        return failureStorage.loadIndexFailure();
    }

    public void cleanupStorage() throws IOException
    {
        cleanupStorage( getDirectory() );
    }

    public static void cleanupStorage( final Directory directory ) throws IOException
    {
        for ( final String fileName : directory.listAll() )
        {
            windowsSafeIOOperation( () -> directory.deleteFile( fileName ) );
        }
    }

    public void openDirectory() throws IOException
    {
        directory = directoryFactory.open( folderLayout.getIndexFolder() );
    }
}
