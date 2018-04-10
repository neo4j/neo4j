/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;
import java.util.Optional;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

class StoreResource
{
    private final File file;
    private final String path;
    private final int recordSize;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;

    StoreResource( File file, String relativePath, int recordSize, PageCache pageCache, FileSystemAbstraction fs )
    {
        this.file = file;
        this.path = relativePath;
        this.recordSize = recordSize;
        this.pageCache = pageCache;
        this.fs = fs;
    }

    ReadableByteChannel open() throws IOException
    {
        if ( !pageCache.fileSystemSupportsFileOperations() )
        {
            Optional<PagedFile> existingMapping = pageCache.getExistingMapping( file );
            if ( existingMapping.isPresent() )
            {
                try ( PagedFile pagedFile = existingMapping.get() )
                {
                    return pagedFile.openReadableByteChannel();
                }
            }
        }

        return fs.open( file, OpenMode.READ );
    }

    public String path()
    {
        return path;
    }

    int recordSize()
    {
        return recordSize;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        StoreResource that = (StoreResource) o;
        return recordSize == that.recordSize && Objects.equals( file, that.file ) && Objects.equals( path, that.path );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( file, path, recordSize );
    }

    @Override
    public String toString()
    {
        return "StoreResource{" + "path='" + path + '\'' + ", recordSize=" + recordSize + '}';
    }
}
