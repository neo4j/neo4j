/*
 * Copyright (c) "Neo4j"
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
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.io.fs.FileUtils;

/**
 * File layout representation of the particular database. Facade for any kind of file lookup for a particular database storage implementation.
 * Any file retrieved from a layout can be considered a canonical file.
 * <br/>
 * No assumption should be made about where and how files of a particular database are positioned and all those details should be encapsulated inside.
 *
 * @see Neo4jLayout
 * @see DatabaseFile
 */
public class DatabaseLayout
{
    private static final String DATABASE_LOCK_FILENAME = "database_lock";
    private static final String BACKUP_TOOLS_FOLDER = "tools";
    private static final String QUARANTINE_MARKER_FILENAME = "quarantine_marker";

    private final Path databaseDirectory;
    private final Neo4jLayout neo4jLayout;
    private final String databaseName;

    public static DatabaseLayout ofFlat( Path databaseDirectory )
    {
        Path canonical = FileUtils.getCanonicalFile( databaseDirectory );
        Path home = canonical.getParent();
        String dbName = canonical.getFileName().toString();
        return Neo4jLayout.ofFlat( home ).databaseLayout( dbName );
    }

    public static DatabaseLayout of( Config config )
    {
        return Neo4jLayout.of( config ).databaseLayout( config.get( GraphDatabaseSettings.default_database ) );
    }

    public static DatabaseLayout of( Neo4jLayout neo4jLayout, String databaseName )
    {
        return new DatabaseLayout( neo4jLayout, databaseName );
    }

    protected DatabaseLayout( Neo4jLayout neo4jLayout, String databaseName )
    {
        var normalizedName = new NormalizedDatabaseName( databaseName ).name();
        this.neo4jLayout = neo4jLayout;
        this.databaseDirectory = FileUtils.getCanonicalFile( neo4jLayout.databasesDirectory().resolve( normalizedName ) );
        this.databaseName = normalizedName;
    }

    public Path getTransactionLogsDirectory()
    {
        return neo4jLayout.transactionLogsRootDirectory().resolve( getDatabaseName() );
    }

    public Path getScriptDirectory()
    {
        return neo4jLayout.scriptRootDirectory().resolve( getDatabaseName() );
    }

    public Path databaseLockFile()
    {
        return databaseDirectory().resolve( DATABASE_LOCK_FILENAME );
    }

    public Path quarantineMarkerFile()
    {
        return databaseDirectory().resolve( QUARANTINE_MARKER_FILENAME );
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public Neo4jLayout getNeo4jLayout()
    {
        return neo4jLayout;
    }

    public Path databaseDirectory()
    {
        return databaseDirectory;
    }

    public Path backupToolsFolder()
    {
        return databaseDirectory().resolve( BACKUP_TOOLS_FOLDER );
    }

    //Index files
    public Path labelScanStore()
    {
        return file( CommonDatabaseFile.LABEL_SCAN_STORE.getName() );
    }

    public Path relationshipTypeScanStore()
    {
        return file( CommonDatabaseFile.RELATIONSHIP_TYPE_SCAN_STORE.getName() );
    }

    public Path indexStatisticsStore()
    {
        return file( CommonDatabaseFile.INDEX_STATISTICS_STORE.getName() );
    }

    public Path metadataStore()
    {
        return file( CommonDatabaseFile.METADATA_STORE.getName() );
    }

    public Set<Path> idFiles()
    {
        return databaseFiles()
                .filter( DatabaseFile::hasIdFile )
                .flatMap( value -> idFile( value ).stream() )
                .collect( Collectors.toSet() );
    }

    public Set<Path> storeFiles()
    {
        return databaseFiles()
                .map( this::file )
                .collect( Collectors.toSet() );
    }

    protected Stream<DatabaseFile> databaseFiles()
    {
        throw new IllegalStateException( "Can not check database files for a plain DatabaseLayout." );
    }

    public Optional<Path> idFile( DatabaseFile file )
    {
        return file.hasIdFile() ? Optional.of( idFile( file.getName() ) ) : Optional.empty();
    }

    public Path file( String fileName )
    {
        return databaseDirectory.resolve( fileName );
    }

    public Path file( DatabaseFile databaseFile )
    {
        return file( databaseFile.getName() );
    }

    public Stream<Path> allFiles( DatabaseFile databaseFile )
    {
        return Stream.concat( idFile( databaseFile ).stream(), Stream.of( file( databaseFile ) ) );
    }

    public Path[] listDatabaseFiles( Predicate<? super Path> filter )
    {
        try ( Stream<Path> list = Files.list( databaseDirectory ) )
        {
            return list.filter( filter ).toArray( Path[]::new );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    protected Path idFile( String name )
    {
        return file( idFileName( name ) );
    }

    private static String idFileName( String storeName )
    {
        return storeName + ".id";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseDirectory, neo4jLayout );
    }

    @Override
    public String toString()
    {
        return "DatabaseLayout{" + "databaseDirectory=" + databaseDirectory + ", transactionLogsDirectory=" + getTransactionLogsDirectory() + '}';
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
        return Objects.equals( databaseDirectory, that.databaseDirectory ) &&
               Objects.equals( neo4jLayout, that.neo4jLayout ) &&
               getTransactionLogsDirectory().equals( that.getTransactionLogsDirectory() );
    }
}
