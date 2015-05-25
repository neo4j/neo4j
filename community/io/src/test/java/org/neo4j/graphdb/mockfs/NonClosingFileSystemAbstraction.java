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
package org.neo4j.graphdb.mockfs;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * {@link FileSystemAbstraction} that wraps another {@link FileSystemAbstraction} with the sole purpose
 * of not calling {@link #close()}. The use case is where a custom FS is provided externally onto the
 * GraphDatabaseFactory, at which point it isn't up to the database to close this file system.
 * This wrapping is fine since it's only done in testing anyway.
 */
public class NonClosingFileSystemAbstraction extends DelegatingFileSystemAbstraction
{
    public NonClosingFileSystemAbstraction( FileSystemAbstraction delegate )
    {
        super( delegate );
    }

    @Override
    public void close() throws IOException
    {
        // Just don't delegate this call.
    }
}
