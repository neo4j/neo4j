/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.io.pagecache.impl.muninn;

import java.nio.file.Path;

final class FileMapping
{
    public volatile FileMapping next;
    public final Path path;
    public final MuninnPagedFile pagedFile;

    FileMapping( Path path, MuninnPagedFile pagedFile )
    {
        this.path = path;
        this.pagedFile = pagedFile;
    }

    @Override
    public String toString()
    {
        return String.format( "FileMapping[fname = %s, refCount = %s] :: %s",
                path, pagedFile.getRefCount(), next );
    }
}
