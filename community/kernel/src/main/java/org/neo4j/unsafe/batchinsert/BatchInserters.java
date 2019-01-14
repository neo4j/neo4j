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
package org.neo4j.unsafe.batchinsert;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.unsafe.batchinsert.internal.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.internal.FileSystemClosingBatchInserter;
import org.neo4j.unsafe.batchinsert.internal.IndexConfigStoreProvider;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Provides instances of {@link BatchInserter}.
 */
public final class BatchInserters
{
    /**
     * Get a {@link BatchInserter} given a store directory.
     *
     * @param storeDir the store directory
     * @return a new {@link BatchInserter}
     * @throws IOException if there is an IO error
     */
    public static BatchInserter inserter( File storeDir ) throws IOException
    {
        DefaultFileSystemAbstraction fileSystem = createFileSystem();
        BatchInserter batchInserter = inserter( storeDir, fileSystem, stringMap() );
        return new FileSystemClosingBatchInserter( batchInserter, (IndexConfigStoreProvider) batchInserter, fileSystem );
    }

    public static BatchInserter inserter( File storeDir, FileSystemAbstraction fs ) throws IOException
    {
        return inserter( storeDir, fs, stringMap(), loadKernelExtension() );
    }

    public static BatchInserter inserter( File storeDir, Map<String,String> config ) throws IOException
    {
        DefaultFileSystemAbstraction fileSystem = createFileSystem();
        BatchInserter inserter = inserter( storeDir, fileSystem, config, loadKernelExtension() );
        return new FileSystemClosingBatchInserter( inserter, (IndexConfigStoreProvider) inserter, fileSystem );
    }

    public static BatchInserter inserter( File storeDir, FileSystemAbstraction fs, Map<String,String> config ) throws IOException
    {
        return inserter( storeDir, fs, config, loadKernelExtension() );
    }

    public static BatchInserter inserter( File storeDir,
            Map<String, String> config, Iterable<KernelExtensionFactory<?>> kernelExtensions ) throws IOException
    {
        DefaultFileSystemAbstraction fileSystem = createFileSystem();
        BatchInserterImpl inserter = new BatchInserterImpl( storeDir, fileSystem, config, kernelExtensions );
        return new FileSystemClosingBatchInserter( inserter, inserter, fileSystem );
    }

    public static BatchInserter inserter( File storeDir, FileSystemAbstraction fileSystem, Map<String,String> config,
            Iterable<KernelExtensionFactory<?>> kernelExtensions ) throws IOException
    {
        return new BatchInserterImpl( storeDir, fileSystem, config, kernelExtensions );
    }

    private static DefaultFileSystemAbstraction createFileSystem()
    {
        return new DefaultFileSystemAbstraction();
    }

    private static Iterable loadKernelExtension()
    {
        return Service.load( KernelExtensionFactory.class );
    }
}
