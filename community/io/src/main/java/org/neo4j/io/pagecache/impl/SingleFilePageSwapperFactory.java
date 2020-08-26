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
package org.neo4j.io.pagecache.impl;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;

/**
 * A factory for SingleFilePageSwapper instances.
 *
 * @see org.neo4j.io.pagecache.impl.SingleFilePageSwapper
 */
public class SingleFilePageSwapperFactory implements PageSwapperFactory
{
    private final FileSystemAbstraction fs;

    public SingleFilePageSwapperFactory( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    @Override
    public PageSwapper createPageSwapper(
            Path file,
            int filePageSize,
            PageEvictionCallback onEviction,
            boolean createIfNotExist,
            boolean useDirectIO ) throws IOException
    {
        if ( !createIfNotExist && !fs.fileExists( file ) )
        {
            throw new NoSuchFileException( file.toString(), null, "Cannot map non-existing file" );
        }
        return new SingleFilePageSwapper( file, fs, filePageSize, onEviction, useDirectIO );
    }

    @Override
    public void close()
    {
        // We have nothing to close
    }
}
