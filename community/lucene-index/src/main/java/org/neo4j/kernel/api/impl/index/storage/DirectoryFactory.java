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
package org.neo4j.kernel.api.impl.index.storage;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.index.storage.paged.PagedDirectory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.util.CustomIOConfigValidator;
import org.neo4j.logging.Log;
import org.neo4j.util.FeatureToggles;

public interface DirectoryFactory extends AutoCloseable
{
    int MAX_MERGE_SIZE_MB = FeatureToggles.getInteger( DirectoryFactory.class, "max_merge_size_mb", 5 );
    int MAX_CACHED_MB = FeatureToggles.getInteger( DirectoryFactory.class, "max_cached_mb", 50 );
    String DEFAULT_FACTORY = FeatureToggles.getString( DirectoryFactory.class, "factory", "paged" );

    Directory open( File dir ) throws IOException;

    /**
     * Called when the directory factory is disposed of, really only here to allow
     * the ram directory thing to close open directories.
     */
    @Override
    void close();

    static DirectoryFactory newDirectoryFactory( PageCache pageCache, Config config, Log log )
    {
        boolean isEphemeral = config.get( GraphDatabaseFacadeFactory.Configuration.ephemeral );
        boolean isUsingCustomIOConfig = CustomIOConfigValidator.customIOConfigUsed( config );

        if ( isEphemeral )
        {
            return new PagedDirectoryFactory( pageCache );
        }

        DirectoryFactory factory;
        switch ( DEFAULT_FACTORY )
        {
        case "heap":
            factory = new HeapDirectoryFactory();
            break;
        case "paged":
            if ( !isUsingCustomIOConfig )
            {
                factory = new PagedDirectoryFactory( pageCache );
                break;
            }
            log.warn( "Paged indexes are not available when custom IO is in use. Most likely this is because you're " +
                    "running on special hardware. Falling back to memory-mapped indexes." );
        case "mmap":
            factory = new MemoryMappedDirectoryFactory();
            break;
        default:
            log.warn( "Unknown index IO implementation '%s', using default.\n", DEFAULT_FACTORY );
            factory = new MemoryMappedDirectoryFactory();
        }
        return new NRTCachingDirectoryFactory( factory );
    }

    /**
     * The default - reads are served via OS memory mapping of Lucenes immutable index segment files,
     * writes via FileChannel.
     */
    final class MemoryMappedDirectoryFactory implements DirectoryFactory
    {
        @SuppressWarnings( "ResultOfMethodCallIgnored" )
        @Override
        public Directory open( File dir ) throws IOException
        {
            Files.createDirectories( dir.toPath() );
            return FSDirectory.open( dir.toPath() );
        }

        @Override
        public void close()
        {
            // No resources to release. This method only exists as a hook for test implementations.
        }
    }

    /**
     * Serves reads directly from file system, via heap buffers.
     */
    final class HeapDirectoryFactory implements DirectoryFactory
    {
        @SuppressWarnings( "ResultOfMethodCallIgnored" )
        @Override
        public Directory open( File dir ) throws IOException
        {
            Files.createDirectories( dir.toPath() );
            return new NIOFSDirectory( dir.toPath() );
        }

        @Override
        public void close()
        {
            // No resources to release. This method only exists as a hook for test implementations.
        }
    }

    /**
     * Produces directories that read via the provided page cache, meaning, for the read path, this
     * has good performance and fixed memory use. The write path could still use some optimization.
     */
    final class PagedDirectoryFactory implements DirectoryFactory
    {
        private final PageCache pageCache;

        public PagedDirectoryFactory( PageCache pageCache )
        {
            this.pageCache = pageCache;
        }

        @Override
        public Directory open( File dir )
        {
            return new PagedDirectory( dir.toPath(), pageCache );
        }

        @Override
        public void close()
        {

        }
    }

    /**
     * Lucene creates one immutable file for every commit - lots of tiny files. Over time, it merges these files into
     * larger ones, but the point remains that lots of tiny files are part of the basic operation of the index.
     * <p>
     * This class limits the actual physical file creation by caching the tiny files in heap buffers, attempting
     * to merge them together into larger files, that in turn actually get written to disk.
     */
    final class NRTCachingDirectoryFactory implements DirectoryFactory
    {
        private final DirectoryFactory delegate;

        public NRTCachingDirectoryFactory( DirectoryFactory delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public Directory open( File dir ) throws IOException
        {
            return new NRTCachingDirectory( delegate.open( dir ), MAX_MERGE_SIZE_MB, MAX_CACHED_MB );
        }

        @Override
        public void close()
        {

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
