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

import java.io.File;
import java.util.Objects;

import static org.neo4j.io.fs.FileUtils.getCanonicalFile;

/**
 * File layout representation of neo4j store that provides the ability to reference any store
 * specific file that can be created by particular store implementation.
 * <br/>
 * <b>Any file lookup should use provided store layout or particular {@link DatabaseLayout database layout}.</b>
 * <br/>
 * Store layout represent layout of whole neo4j store while particular {@link DatabaseLayout database layout} represent single database.
 * Store layout should be used as a factory of any layouts for particular database.
 * <br/>
 * Any user-provided store directory will be transformed to canonical file form and any subsequent store layout file
 * lookup should be considered as operations that provide canonical file form.
 * <br/>
 * Store lock file is global per store and should be looked from specific store layout.
 * <br/>
 * Example of store layout for store with 2 databases:
 * <pre>
 *  store directory
 *  | \ database directory (graph.db, represented by separate database layout)
 *  |    \ particular database files
 *  | \ database directory (other.db represented by separate database layout)
 *  |    \ particular database files
 *  store_lock
 * </pre>
 * The current implementation does not keep references to all requested and provided files and requested layouts but can be easily enhanced to do so.
 *
 * @see DatabaseLayout
 */
public class StoreLayout
{
    private static final String STORE_LOCK_FILENAME = "store_lock";

    private final File storeDirectory;

    public static StoreLayout of( File storeDirectory )
    {
        return new StoreLayout( getCanonicalFile( storeDirectory ) );
    }

    private StoreLayout( File rootStoreDirectory )
    {
        this.storeDirectory = rootStoreDirectory;
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
        return DatabaseLayout.of( storeDirectory, databaseName );
    }

    /**
     * Databases root directory where all databases are located.
     * @return all databases root directory
     */
    public File storeDirectory()
    {
        return storeDirectory;
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
        StoreLayout that = (StoreLayout) o;
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
        return String.valueOf( storeDirectory );
    }
}
