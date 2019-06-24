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
package org.neo4j.batchinsert;

import java.io.IOException;

import org.neo4j.batchinsert.internal.BatchInserterImpl;
import org.neo4j.batchinsert.internal.FileSystemClosingBatchInserter;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.service.Services;

/**
 * Provides instances of {@link BatchInserter}.
 */
public final class BatchInserters
{
    /**
     * Get a {@link BatchInserter} given a store directory.
     *
     * @param databaseLayout directory where particular neo4j database is located
     * @return a new {@link BatchInserter}
     * @throws IOException if there is an IO error
     */
    public static BatchInserter inserter( DatabaseLayout databaseLayout ) throws IOException
    {
        DefaultFileSystemAbstraction fileSystem = createFileSystem();
        BatchInserter batchInserter = inserter( databaseLayout, fileSystem, Config.defaults() );
        return new FileSystemClosingBatchInserter( batchInserter, fileSystem );
    }

    public static BatchInserter inserter( DatabaseLayout databaseLayout, FileSystemAbstraction fs ) throws IOException
    {
        return inserter( databaseLayout, fs, Config.defaults(), loadExtension() );
    }

    public static BatchInserter inserter( DatabaseLayout databaseLayout, Config config ) throws IOException
    {
        DefaultFileSystemAbstraction fileSystem = createFileSystem();
        BatchInserter inserter = inserter( databaseLayout, fileSystem, config, loadExtension() );
        return new FileSystemClosingBatchInserter( inserter, fileSystem );
    }

    public static BatchInserter inserter( DatabaseLayout databaseLayout, FileSystemAbstraction fs, Config config ) throws IOException
    {
        return inserter( databaseLayout, fs, config, loadExtension() );
    }

    public static BatchInserter inserter( DatabaseLayout databaseLayout,
            Config config, Iterable<ExtensionFactory<?>> extensions ) throws IOException
    {
        DefaultFileSystemAbstraction fileSystem = createFileSystem();
        BatchInserterImpl inserter = new BatchInserterImpl( databaseLayout, fileSystem, config, extensions );
        return new FileSystemClosingBatchInserter( inserter, fileSystem );
    }

    public static BatchInserter inserter( DatabaseLayout layout, FileSystemAbstraction fileSystem, Config config,
            Iterable<ExtensionFactory<?>> extensions ) throws IOException
    {
        return new BatchInserterImpl( layout, fileSystem, config, extensions );
    }

    private static DefaultFileSystemAbstraction createFileSystem()
    {
        return new DefaultFileSystemAbstraction();
    }

    private static Iterable loadExtension()
    {
        return Services.loadAll( ExtensionFactory.class );
    }
}
