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
package org.neo4j.io.layout;

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.util.Collection;
import java.util.Objects;

import org.neo4j.io.fs.FileUtils;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.neo4j.io.layout.StoreLayoutConfig.NOT_CONFIGURED;

/**
 * File layout representation of neo4j instance that provides the ability to reference any store
 * specific file that can be created by particular store implementation.
 * <br/>
 * <b>Any file lookup should use provided layout or particular {@link DatabaseLayout database layout}.</b>
 * <br/>
 * Neo4J layout represent layout of whole neo4j instance while particular {@link DatabaseLayout database layout} represent single database.
 * Neo4J layout should be used as a factory of any layouts for particular database.
 * <br/>
 * Any user-provided home or store directory will be transformed to canonical file form and any subsequent layout file
 * lookup should be considered as operations that provide canonical file form.
 * <br/>
 * Store lock file is global per store and should be looked from specific store layout.
 * <br/>
 * Example of Neo4j layout for store with 2 databases:
 * <pre>
 *  home directory
 *  | \ data directory
 *  | | \ store directory
 *  | | | \ database directory (neo4j, represented by separate database layout)
 *  | | | | \ particular database files
 *  | | | \ database directory (other represented by separate database layout)
 *  | | | | \ particular database files
 *  | | | \ store_lock
 *  | | \ transaction logs directory
 *  | | | \ database tx-logs directory (neo4j, represented by separate database layout)
 *  | | | | \ particular database tx-logs files
 *  | | | \ database tx-logs directory (other represented by separate database layout)
 *  | | | | \ particular database tx-logs files
 * </pre>
 * The current implementation does not keep references to all requested and provided files and requested layouts but can be easily enhanced to do so.
 * <br/>
 * Most file & directory locations of the layout can be individually configured using their corresponding setting.
 *
 * @see DatabaseLayout
 */
public class Neo4jLayout
{
    private static final String STORE_LOCK_FILENAME = "store_lock";

    private final File homeDirectory;
    private final File storeDirectory;
    private final StoreLayoutConfig layoutConfig;

    public static Neo4jLayout of( File homeDirectory, File storeDirectory )
    {
        return of( homeDirectory, storeDirectory, NOT_CONFIGURED  );
    }

    public static Neo4jLayout of( File homeDirectory, File storeDirectory, StoreLayoutConfig config )
    {
        return new Neo4jLayout( homeDirectory, storeDirectory, config );
    }

    private Neo4jLayout( File homeDirectory, File rootStoreDirectory, StoreLayoutConfig layoutConfig )
    {
        this.homeDirectory = FileUtils.getCanonicalFile( homeDirectory );
        this.storeDirectory = FileUtils.getCanonicalFile( rootStoreDirectory );
        this.layoutConfig = layoutConfig;
    }

    /**
     * Try to return database layouts for directories that located in the current store directory.
     * Each sub directory of the store directory treated as a separate database directory and database layout wrapper build for that.
     *
     * @return database layouts for directories located in current store directory. If no subdirectories exist empty collection is returned.
     */
    public Collection<DatabaseLayout> databaseLayouts()
    {
        File[] directories = storeDirectory.listFiles( File::isDirectory );
        if ( ArrayUtils.isEmpty( directories ) )
        {
            return emptyList();
        }
        return stream( directories ).map( directory -> DatabaseLayout.of( this, directory.getName() ) ).collect( toList() );
    }

    /**
     * Provide layout for a database with provided name.
     * No assumptions whatsoever should be taken in regards of database location.
     * Newly created layout should be used to any kind of file related requests in scope of a database.
     * @param databaseName database name to provide layout for
     * @return requested database layout
     */
    public DatabaseLayout databaseLayout( String databaseName )
    {
        return DatabaseLayout.of( this, databaseName );
    }

    /**
     * Databases root directory where all databases are located.
     * @return all databases root directory
     */
    public File storeDirectory()
    {
        return storeDirectory;
    }

    /**
     * Neo4J root directory.
     * @return the root of the Neo4j instance
     */
    public File homeDirectory()
    {
        return homeDirectory;
    }

    /**
     * Configuration of store layout
     * @return layout config
     */
    StoreLayoutConfig getLayoutConfig()
    {
        return layoutConfig;
    }

    public File transactionLogsRootDirectory()
    {
        return getLayoutConfig().getTransactionLogsRootDirectory().orElse( storeDirectory() );
    }

    public File storeLockFile()
    {
        return new File( storeDirectory, STORE_LOCK_FILENAME );
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
        Neo4jLayout that = (Neo4jLayout) o;
        return Objects.equals( storeDirectory, that.storeDirectory );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( storeDirectory );
    }

    @Override
    public String toString()
    {
        return "Neo4jLayout{" + "storeDirectory=" + storeDirectory + ", transactionLogsRootDirectory=" + layoutConfig.getTransactionLogsRootDirectory() + '}';
    }
}
