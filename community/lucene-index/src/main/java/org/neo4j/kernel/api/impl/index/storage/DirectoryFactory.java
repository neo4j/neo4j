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
package org.neo4j.kernel.api.impl.index.storage;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.util.FeatureToggles;

public interface DirectoryFactory extends AutoCloseable
{
    static DirectoryFactory directoryFactory( boolean ephemeral )
    {
        return ephemeral ? new DirectoryFactory.InMemoryDirectoryFactory() : DirectoryFactory.PERSISTENT;
    }

    Directory open( File dir ) throws IOException;

    DirectoryFactory PERSISTENT = new DirectoryFactory()
    {
        private final int MAX_MERGE_SIZE_MB =
                FeatureToggles.getInteger( DirectoryFactory.class, "max_merge_size_mb", 5 );
        private final int MAX_CACHED_MB =
                FeatureToggles.getInteger( DirectoryFactory.class, "max_cached_mb", 50 );
        private final boolean USE_DEFAULT_DIRECTORY_FACTORY =
                FeatureToggles.flag( DirectoryFactory.class, "default_directory_factory", true );

        @SuppressWarnings( "ResultOfMethodCallIgnored" )
        @Override
        public Directory open( File dir ) throws IOException
        {
            dir.mkdirs();
            FSDirectory directory = USE_DEFAULT_DIRECTORY_FACTORY ? FSDirectory.open( dir.toPath() ) : new NIOFSDirectory( dir.toPath() );
            return new NRTCachingDirectory( directory, MAX_MERGE_SIZE_MB, MAX_CACHED_MB );
        }

        @Override
        public void close()
        {
            // No resources to release. This method only exists as a hook for test implementations.
        }

    };

    final class InMemoryDirectoryFactory implements DirectoryFactory
    {
        private final Map<File, RAMDirectory> directories = new HashMap<>();

        @Override
        public synchronized Directory open( File dir )
        {
            if ( !directories.containsKey( dir ) )
            {
                directories.put( dir, new RAMDirectory() );
            }
            return new UncloseableDirectory( directories.get( dir ) );
        }

        @Override
        public synchronized void close()
        {
            for ( RAMDirectory ramDirectory : directories.values() )
            {
                ramDirectory.close();
            }
            directories.clear();
        }
    }

    final class Single implements DirectoryFactory
    {
        private final Directory directory;

        public Single( Directory directory )
        {
            this.directory = directory;
        }

        @Override
        public Directory open( File dir )
        {
            return directory;
        }

        @Override
        public void close()
        {
        }
    }

    final class UncloseableDirectory extends FilterDirectory
    {

        public UncloseableDirectory( Directory delegate )
        {
            super( delegate );
        }

        @Override
        public void close()
        {
            // No-op
        }
    }
}
