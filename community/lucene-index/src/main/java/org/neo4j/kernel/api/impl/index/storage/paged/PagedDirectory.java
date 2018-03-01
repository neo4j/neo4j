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
package org.neo4j.kernel.api.impl.index.storage.paged;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.READ;

public class PagedDirectory extends BaseDirectory
{
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;

    private final Path path; // The underlying filesystem path

    public PagedDirectory( Path path, PageCache pageCache )
    {
        // TODO: We can probably use SingleInstanceLockFactory here
        super( new FSALockFactory( pageCache.getCachedFileSystem() ) );
        this.pageCache = pageCache;
        this.fs = pageCache.getCachedFileSystem();
        this.path = path;
    }

    @Override
    public IndexInput openInput( String name, IOContext context ) throws IOException
    {
        ensureOpen();
        File file = path.resolve( name ).toFile();
        PagedFile pagedFile = pageCache.map( file, pageCache.pageSize(), READ );
        long size = fs.getFileSize( file );
        return new PagedIndexInput( "PagedIndexInput(\"" + path.resolve( name ).toString() + "\")", pagedFile, 0,
                size );
    }

    @Override
    public String[] listAll() throws IOException
    {
        ensureOpen();
        return fs.streamFilesRecursive( path.toFile() ).map( f -> f.getFile().getName() ).toArray( String[]::new );
    }

    @Override
    public long fileLength( String name )
    {
        ensureOpen();
        return fs.getFileSize( path.resolve( name ).toFile() );
    }

    @Override
    public void deleteFile( String name ) throws IOException
    {
        ensureOpen();
        Path filePath = path.resolve( name );
        if ( !fs.deleteFile( filePath.toFile() ) )
        {
            throw new IOException( "Unable to delete " + filePath );
        }
    }

    @Override
    public IndexOutput createOutput( String name, IOContext context ) throws IOException
    {
        ensureOpen();
        return new PagedIndexOutput( path.resolve( name ), fs );
    }

    @Override
    public void sync( Collection<String> names ) throws IOException
    {
        ensureOpen();
        for ( String name : names )
        {
            fs.force( path.resolve( name ).toFile() );
        }
    }

    @Override
    public void renameFile( String source, String dest ) throws IOException
    {
        ensureOpen();
        fs.renameFile( path.resolve( source ).toFile(), path.resolve( dest ).toFile(), ATOMIC_MOVE );
        fs.force( path.toFile() );
    }

    @Override
    public void close()
    {
        isOpen = false;
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + "@" + path + " lockFactory=" + lockFactory;
    }

    public Path getPath()
    {
        return path;
    }
}
