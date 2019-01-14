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
package org.neo4j.io.fs;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

import org.neo4j.graphdb.mockfs.CloseTrackingFileSystem;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DelegateFileSystemAbstractionTest
{

    @Test
    public void closeAllResourcesOnClose() throws Exception
    {
        TrackableFileSystem fileSystem = new TrackableFileSystem();
        CloseTrackingFileSystem closeTrackingFileSystem = new CloseTrackingFileSystem();

        try ( DelegateFileSystemAbstraction fileSystemAbstraction = new DelegateFileSystemAbstraction( fileSystem ) )
        {
            fileSystemAbstraction.getOrCreateThirdPartyFileSystem( CloseTrackingFileSystem.class,
                    closeTrackingFileSystemClass -> closeTrackingFileSystem );
        }

        assertFalse( fileSystem.isOpen() );
        assertTrue( closeTrackingFileSystem.isClosed() );
    }

    @Test
    public void delegatedFileSystemWatcher() throws IOException
    {
        FileSystem fileSystem = mock(FileSystem.class);
        try ( DelegateFileSystemAbstraction abstraction = new DelegateFileSystemAbstraction( fileSystem ) )
        {
            assertNotNull( abstraction.fileWatcher() );
        }

        verify( fileSystem ).newWatchService();
    }

    private static class TrackableFileSystem extends FileSystem
    {

        private boolean closed;

        @Override
        public FileSystemProvider provider()
        {
            return null;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public boolean isOpen()
        {
            return !closed;
        }

        @Override
        public boolean isReadOnly()
        {
            return false;
        }

        @Override
        public String getSeparator()
        {
            return null;
        }

        @Override
        public Iterable<Path> getRootDirectories()
        {
            return null;
        }

        @Override
        public Iterable<FileStore> getFileStores()
        {
            return null;
        }

        @Override
        public Set<String> supportedFileAttributeViews()
        {
            return null;
        }

        @Override
        public Path getPath( String first, String... more )
        {
            return null;
        }

        @Override
        public PathMatcher getPathMatcher( String syntaxAndPattern )
        {
            return null;
        }

        @Override
        public UserPrincipalLookupService getUserPrincipalLookupService()
        {
            return null;
        }

        @Override
        public WatchService newWatchService()
        {
            return null;
        }
    }
}
