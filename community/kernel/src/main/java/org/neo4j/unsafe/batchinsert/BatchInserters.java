/**
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
package org.neo4j.unsafe.batchinsert;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

/**
 * Provides instances of batch inserters.
 * <p>
 * A {@link BatchInserter} retrieved from the {@link #inserter(String)} or
 * {@link #inserter(String, Map)} methods is more performant while the
 * {@link GraphDatabaseService} retrievied from {@link #batchDatabase(String)}
 * or {@link #batchDatabase(String, Map)} methods is there for convenience, so
 * you can reuse existing code.
 */
public final class BatchInserters
{
    /**
     * Get a {@link BatchInserter} given a store directory.
     * 
     * @param storeDir the store directory
     * @return a new {@link BatchInserter}
     */
    public static BatchInserter inserter( String storeDir )
    {
        return inserter( storeDir, stringMap() );
    }

    /**
     * Get a {@link BatchInserter} given a store directory.
     * 
     * @param storeDir the store directory
     * @param config configuration settings to use
     * @return a new {@link BatchInserter}
     */
    public static BatchInserter inserter( String storeDir, Map<String,String> config )
    {
        return inserter( storeDir, new DefaultFileSystemAbstraction(), config );
    }

    /**
     * Get a {@link BatchInserter} given a store directory.
     * 
     * @param storeDir the store directory
     * @return a new {@link BatchInserter}
     */
    public static BatchInserter inserter( String storeDir, FileSystemAbstraction fileSystem )
    {
        return inserter( storeDir, fileSystem, stringMap() );
    }

    /**
     * Get a {@link BatchInserter} given a store directory.
     * 
     * @param storeDir the store directory
     * @param config configuration settings to use
     * @return a new {@link BatchInserter}
     */
    @SuppressWarnings( { "unchecked", "rawtypes" } )
    public static BatchInserter inserter( String storeDir, FileSystemAbstraction fileSystem,
            Map<String,String> config )
    {
        return inserter( storeDir, fileSystem, config, (Iterable) Service.load( KernelExtensionFactory.class ) );
    }
    
    public static BatchInserter inserter( String storeDir, FileSystemAbstraction fileSystem,
            Map<String, String> config, Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        return new BatchInserterImpl( storeDir, fileSystem, config, kernelExtensions );
    }
    
    /**
     * Get a {@link GraphDatabaseService} that does not support deletions and
     * transactions.
     * 
     * @param storeDir the store directory
     * @return a {@link GraphDatabaseService} that does not support deletions
     *         and transactions
     */
    public static GraphDatabaseService batchDatabase( String storeDir )
    {
        return batchDatabase( storeDir, stringMap() );
    }

    /**
     * Get a {@link GraphDatabaseService} that does not support deletions and
     * transactions.
     * 
     * @param storeDir the store directory
     * @param config configuration settings to use
     * @return a {@link GraphDatabaseService} that does not support deletions
     *         and transactions
     */
    public static GraphDatabaseService batchDatabase( String storeDir,
            Map<String, String> config )
    {
        return batchDatabase( storeDir, new DefaultFileSystemAbstraction(), config );
    }

    /**
     * Get a {@link GraphDatabaseService} that does not support deletions and
     * transactions.
     * 
     * @param storeDir the store directory
     * @return a {@link GraphDatabaseService} that does not support deletions
     *         and transactions
     */
    public static GraphDatabaseService batchDatabase( String storeDir, FileSystemAbstraction fileSystem )
    {
        return batchDatabase( storeDir, fileSystem, stringMap() );
    }

    /**
     * Get a {@link GraphDatabaseService} that does not support deletions and
     * transactions.
     * 
     * @param storeDir the store directory
     * @param config configuration settings to use
     * @return a {@link GraphDatabaseService} that does not support deletions
     *         and transactions
     */
    @SuppressWarnings( { "unchecked", "rawtypes" } )
    public static GraphDatabaseService batchDatabase( String storeDir,
            FileSystemAbstraction fileSystem, Map<String, String> config )
    {
        return batchDatabase( storeDir, fileSystem, config, (Iterable) Service.load( KernelExtensionFactory.class ) );
    }

    public static GraphDatabaseService batchDatabase( String storeDir, FileSystemAbstraction fileSystem,
            Map<String, String> config, Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        return new BatchGraphDatabaseImpl( storeDir, fileSystem, config, kernelExtensions );
    }
}
