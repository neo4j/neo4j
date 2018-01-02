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
package org.neo4j.unsafe.batchinsert;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.KernelExtensionFactory;

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
     * @deprecated use {@link #inserter(File)} instead
     */
    @Deprecated
    public static BatchInserter inserter( String storeDir )
    {
        return inserter( storeDir, stringMap() );
    }

    /**
     * Get a {@link BatchInserter} given a store directory.
     *
     * @param storeDir the store directory
     * @return a new {@link BatchInserter}
     * @throws IOException if there is an IO error
     */
    public static BatchInserter inserter( File storeDir ) throws IOException
    {
        return inserter( storeDir, new DefaultFileSystemAbstraction(), stringMap() );
    }

    /**
     * Get a {@link BatchInserter} given a store directory.
     *
     * @param storeDir the store directory
     * @param config configuration settings to use
     * @return a new {@link BatchInserter}
     * @deprecated use {@link #inserter(File, Map)} instead
     */
    @Deprecated
    public static BatchInserter inserter( String storeDir, Map<String,String> config )
    {
        return inserter( storeDir, new DefaultFileSystemAbstraction(), config );
    }

    /**
     * Get a {@link BatchInserter} given a store directory.
     *
     * @param storeDir the store directory
     * @param config configuration settings to use
     * @return a new {@link BatchInserter}
     * @throws IOException if there is an IO error
     */
    public static BatchInserter inserter( File storeDir, Map<String,String> config ) throws IOException
    {
        return inserter( storeDir, new DefaultFileSystemAbstraction(), config );
    }

    /**
     * @deprecated use {@link #inserter(File)} instead
     * @param storeDir the store directory
     * @param fileSystem the file system abstraction to use
     * @return a new {@link BatchInserter}
     */
    @Deprecated
    public static BatchInserter inserter( String storeDir, FileSystemAbstraction fileSystem )
    {
        return inserter( storeDir, fileSystem, stringMap() );
    }

    /**
     * Get a {@link BatchInserter} given a store directory.
     *
     * @param storeDir the store directory
     * @param fileSystem the file system abstraction to use
     * @return a new {@link BatchInserter}
     * @deprecated use {@link #inserter(File)} instead
     * @throws IOException if there is an IO error
     */
    @Deprecated
    public static BatchInserter inserter( File storeDir, FileSystemAbstraction fileSystem ) throws IOException
    {
        return inserter( storeDir, fileSystem, stringMap() );
    }

    /**
     * @deprecated use {@link #inserter(File, Map)} instead
     * @param storeDir the store directory
     * @param fileSystem the file system abstraction to use
     * @param config configuration settings to use
     * @return a new {@link BatchInserter}
     */
    @Deprecated
    public static BatchInserter inserter( String storeDir, FileSystemAbstraction fileSystem,
            Map<String,String> config )
    {
        try
        {
            return inserter( new File( storeDir ), fileSystem, config );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Get a {@link BatchInserter} given a store directory.
     *
     * @param storeDir the store directory
     * @param fileSystem the file system abstraction to use
     * @param config configuration settings to use
     * @return a new {@link BatchInserter}
     * @deprecated use {@link #inserter(File, Map)} instead
     * @throws IOException if there is an IO error
     */
    @Deprecated
    @SuppressWarnings( { "unchecked", "rawtypes" } )
    public static BatchInserter inserter( File storeDir, FileSystemAbstraction fileSystem,
                                          Map<String,String> config ) throws IOException
    {
        return inserter( storeDir, fileSystem, config, (Iterable) Service.load( KernelExtensionFactory.class ) );
    }

    /**
     * @deprecated use {@link #inserter(File, Map, Iterable)} instead
     * @param storeDir the store directory
     * @param fileSystem the file system abstraction to use
     * @param config configuration settings to use
     * @param kernelExtensions kernel extensions
     * @return a new {@link BatchInserter}
     */
    @Deprecated
    public static BatchInserter inserter( String storeDir, FileSystemAbstraction fileSystem,
            Map<String, String> config, Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        try
        {
            return inserter( new File( storeDir ), fileSystem, config, kernelExtensions );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public static BatchInserter inserter( File storeDir,
                                          Map<String, String> config, Iterable<KernelExtensionFactory<?>> kernelExtensions ) throws IOException
    {
        return new BatchInserterImpl( storeDir, new DefaultFileSystemAbstraction(), config, kernelExtensions );
    }

    private static BatchInserter inserter( File storeDir, FileSystemAbstraction fileSystem,
                                          Map<String, String> config, Iterable<KernelExtensionFactory<?>> kernelExtensions ) throws IOException
    {
        return new BatchInserterImpl( storeDir, fileSystem, config, kernelExtensions );
    }
}
