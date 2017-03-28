/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.commandline;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import javax.annotation.Nonnull;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.internal.StoreLocker;

import static java.lang.String.format;

public class Util
{
    public static Path canonicalPath( Path path ) throws IllegalArgumentException
    {
        return canonicalPath( path.toFile() );
    }

    public static Path canonicalPath( String path ) throws IllegalArgumentException
    {
        return canonicalPath( new File( path ) );
    }

    public static Path canonicalPath( File file ) throws IllegalArgumentException
    {
        try
        {
            return Paths.get( file.getCanonicalPath() );
        }
        catch ( IOException e )
        {
            throw new IllegalArgumentException( "Unable to parse path: " + file, e );
        }
    }

    public static void checkLock( Path databaseDirectory ) throws CommandFailed
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              StoreLocker storeLocker = new StoreLocker( fileSystem ) )
        {
            storeLocker.checkLock( databaseDirectory.toFile() );
        }
        catch ( StoreLockException e )
        {
            throw new CommandFailed( "the database is in use -- stop Neo4j and try again", e );
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
    }

    public static void wrapIOException( IOException e ) throws CommandFailed
    {
        throw new CommandFailed(
                format( "unable to load database: %s: %s", e.getClass().getSimpleName(), e.getMessage() ), e );
    }

    /**
     * @return the version of Neo4j as defined during the build
     */
    @Nonnull
    public static String neo4jVersion()
    {
        Properties props = new Properties();
        try
        {
            props.load( Util.class.getResourceAsStream( "/org/neo4j/commandline/build.properties" ) );
            return props.getProperty( "neo4jVersion" );
        }
        catch ( IOException e )
        {
            // This should never happen
            throw new RuntimeException( e );
        }
    }
}
