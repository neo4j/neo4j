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
import java.io.FilenameFilter;
import java.util.Objects;

import static org.neo4j.io.fs.FileUtils.getCanonicalFile;

public class DatabaseLayout
{
    private static final File[] EMPTY_FILES_ARRAY = new File[0];
    private final File databaseDirectory;
    private final StoreLayout storeLayout;
    private final String databaseName;

    public static DatabaseLayout of( StoreLayout storeLayout, String databaseName )
    {
        return new DatabaseLayout( storeLayout, databaseName );
    }

    public static DatabaseLayout of( File databaseDirectory )
    {
        File canonicalFile = getCanonicalFile( databaseDirectory );
        return of( canonicalFile.getParentFile(), canonicalFile.getName() );
    }

    public static DatabaseLayout of( File rootDirectory, String databaseName )
    {
        return new DatabaseLayout( StoreLayout.of( rootDirectory ), databaseName );
    }

    private DatabaseLayout( StoreLayout storeLayout, String databaseName )
    {
        this.storeLayout = storeLayout;
        this.databaseDirectory = new File( storeLayout.storeDirectory(), databaseName );
        this.databaseName = databaseName;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public StoreLayout getStoreLayout()
    {
        return storeLayout;
    }

    public File databaseDirectory()
    {
        return databaseDirectory;
    }

    public File file( String fileName )
    {
        return new File( databaseDirectory, fileName );
    }

    public File directory( String directoryName )
    {
        return new File( databaseDirectory, directoryName );
    }

    public File[] listDatabaseFiles( FilenameFilter filter )
    {
        File[] files = databaseDirectory.listFiles( filter );
        return files != null ? files : EMPTY_FILES_ARRAY;
    }

    @Override
    public String toString()
    {
        return String.valueOf( databaseDirectory );
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
        DatabaseLayout that = (DatabaseLayout) o;
        return Objects.equals( databaseDirectory, that.databaseDirectory ) && Objects.equals( storeLayout, that.storeLayout );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseDirectory, storeLayout );
    }
}
