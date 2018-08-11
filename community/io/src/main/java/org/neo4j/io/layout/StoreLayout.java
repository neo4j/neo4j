/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

public class StoreLayout
{
    private static final String STORE_LOCK_FILENAME = "store_lock";

    private final File storeDirectory;

    public static StoreLayout of( File storeDirectory )
    {
        return new StoreLayout( storeDirectory );
    }

    private StoreLayout( File rootStoreDirectory )
    {
        this.storeDirectory = rootStoreDirectory;
    }

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
