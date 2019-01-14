/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
