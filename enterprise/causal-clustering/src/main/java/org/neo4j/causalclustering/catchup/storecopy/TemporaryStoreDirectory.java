/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;

public class TemporaryStoreDirectory implements AutoCloseable
{
    private static final String TEMP_COPY_DIRECTORY_NAME = "temp-copy";

    private final File tempStoreDir;
    private final StoreFiles storeFiles;

    public TemporaryStoreDirectory( FileSystemAbstraction fs, PageCache pageCache, File parent ) throws IOException
    {
        this.tempStoreDir = new File( parent, TEMP_COPY_DIRECTORY_NAME );
        storeFiles = new StoreFiles( fs, pageCache, ( directory, name ) -> true );
        storeFiles.delete( tempStoreDir );
    }

    public File storeDir()
    {
        return tempStoreDir;
    }

    @Override
    public void close() throws IOException
    {
        storeFiles.delete( tempStoreDir );
    }
}
