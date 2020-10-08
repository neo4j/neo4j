/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

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
 *  | | | \ database transactions directory (neo4j, represented by separate database layout)
 *  | | | | \ particular database transactions files
 *  | | | \ database transactions directory (other represented by separate database layout)
 *  | | | | \ particular database transactions files
 *  | | \ script directory
 *  | | | \ database script directory (neo4j, represented by separate database layout)
 *  | | | | \ particular database script files
 *  | | | \ database script directory (other represented by separate database layout)
 *  | | | | \ particular database script files
 * </pre>
 * The current implementation does not keep references to all requested and provided files and requested layouts but can be easily enhanced to do so.
 * <br/>
 * Most file & directory locations of the layout can be individually configured using their corresponding setting.
 *
 * @see DatabaseLayout
 */
public final class Neo4jLayout
{
    private static final String STORE_LOCK_FILENAME = "store_lock";
    private static final String SERVER_ID_FILENAME = "server_id";

    private final Path homeDirectory;
    private final Path dataDirectory;
    private final Path databasesRootDirectory;
    private final Path txLogsRootDirectory;
    private final Path scriptRootDirectory;

    public static Neo4jLayout of( Path homeDirectory )
    {
        return of( Config.defaults( GraphDatabaseSettings.neo4j_home, FileUtils.getCanonicalFile( homeDirectory ).toAbsolutePath() ) );
    }

    public static Neo4jLayout of( Config config )
    {
        var homeDirectory = config.get( GraphDatabaseSettings.neo4j_home );
        var dataDirectory = config.get( GraphDatabaseSettings.data_directory );
        var databasesRootDirectory = config.get( GraphDatabaseInternalSettings.databases_root_path );
        var txLogsRootDirectory = config.get( GraphDatabaseSettings.transaction_logs_root_path );
        var scriptRootDirectory = config.get( GraphDatabaseSettings.script_root_path );
        return new Neo4jLayout( homeDirectory, dataDirectory, databasesRootDirectory, txLogsRootDirectory, scriptRootDirectory );
    }

    public static Neo4jLayout ofFlat( Path homeDirectory )
    {
        var home = homeDirectory.toAbsolutePath();
        var config = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, home )
                .set( GraphDatabaseSettings.data_directory, home )
                .set( GraphDatabaseSettings.transaction_logs_root_path, home )
                .set( GraphDatabaseInternalSettings.databases_root_path, home )
                .build();
        return of( config );
    }

    private Neo4jLayout( Path homeDirectory, Path dataDirectory, Path databasesRootDirectory, Path txLogsRootDirectory, Path scriptRootDirectory )
    {
        this.homeDirectory = FileUtils.getCanonicalFile( homeDirectory );
        this.dataDirectory = FileUtils.getCanonicalFile( dataDirectory );
        this.databasesRootDirectory = FileUtils.getCanonicalFile( databasesRootDirectory );
        this.txLogsRootDirectory = FileUtils.getCanonicalFile( txLogsRootDirectory );
        this.scriptRootDirectory = FileUtils.getCanonicalFile( scriptRootDirectory );
    }

    /**
     * Try to return database layouts for directories that located in the current store directory.
     * Each sub directory of the store directory treated as a separate database directory and database layout wrapper build for that.
     *
     * @return database layouts for directories located in current store directory. If no subdirectories exist empty collection is returned.
     */
    public Collection<DatabaseLayout> databaseLayouts()
    {
        try ( Stream<Path> list = Files.list( databasesRootDirectory) )
        {
            return list.filter( Files::isDirectory ).map( directory -> DatabaseLayout.of( this, directory.getFileName().toString() ) ).collect( toList() );
        }
        catch ( NoSuchFileException e )
        {
            return emptyList();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
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
    public Path databasesDirectory()
    {
        return databasesRootDirectory;
    }

    /**
     * Neo4J root directory.
     * @return the root of the Neo4j instance
     */
    public Path homeDirectory()
    {
        return homeDirectory;
    }

    public Path transactionLogsRootDirectory()
    {
        return txLogsRootDirectory;
    }

    public Path scriptRootDirectory()
    {
        return scriptRootDirectory;
    }

    public Path dataDirectory()
    {
        return dataDirectory;
    }

    public Path storeLockFile()
    {
        return databasesRootDirectory.resolve( STORE_LOCK_FILENAME );
    }

    public Path serverIdFile()
    {
        return dataDirectory.resolve( SERVER_ID_FILENAME );
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
        var that = (Neo4jLayout) o;
        return Objects.equals( homeDirectory, that.homeDirectory ) && Objects.equals( dataDirectory, that.dataDirectory ) &&
               Objects.equals( databasesRootDirectory, that.databasesRootDirectory ) && Objects.equals( txLogsRootDirectory, that.txLogsRootDirectory );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( homeDirectory, dataDirectory, databasesRootDirectory, txLogsRootDirectory );
    }

    @Override
    public String toString()
    {
        return String.format( "Neo4JLayout{ homeDir=%s, dataDir=%s, databasesDir=%s, txLogsRootDir=%s}",
                homeDirectory.toString(), dataDirectory.toString(), databasesRootDirectory.toString(), txLogsRootDirectory.toString() );
    }
}
