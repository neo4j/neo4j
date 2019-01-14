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
package org.neo4j.upgrade.lucene;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.upgrade.loader.JarLoaderSupplier;

/**
 * Lucene index upgrader that will try to migrate all indexes in specified index root directory.
 * <p>
 * Currently index migration has 2 steps:
 * <ol>
 * <li>Migration to format supported by Lucene 4</li>
 * <li>Migration to format supported by Lucene 5</li>
 * </ol>
 * Migration performed by using native lucene's IndexUpgraders from corresponding versions. For details see Lucenes
 * migration guide.
 * <p>
 * In case if one of the indexes can not be migrated migration is terminated and corresponding exception is thrown.
 */
public class LuceneExplicitIndexUpgrader
{
    public interface Monitor
    {
        /**
         * Upgrade is starting.
         *
         * @param count number of indexes to migrate.
         */
        default void starting( int count ) {}

        /**
         * Called after an index has been migrated, called for each migrated index.
         *
         * @param name name of the index.
         */
        default void migrated( String name ) {}
    }

    public static final Monitor NO_MONITOR = new Monitor()
    {
    };

    private static final String LIBRARY_DIRECTORY = "lib";
    private static final String RESOURCE_SEPARATOR = "/";
    private static final String LUCENE4_CORE_JAR_NAME = "lucene-core-4.10.4.jar";
    private static final String LUCENE5_CORE_JAR_NAME = "lucene-core-5.5.0.jar";
    private static final String LUCENE5_BACKWARD_CODECS_NAME = "lucene-backward-codecs-5.5.0.jar";
    private static final String SEGMENTS_FILE_NAME_PREFIX = "segments";

    private final Path indexRootPath;
    private final Monitor monitor;

    public LuceneExplicitIndexUpgrader( Path indexRootPath, Monitor monitor )
    {
        this.monitor = monitor;
        if ( Files.exists( indexRootPath ) && !Files.isDirectory( indexRootPath ) )
        {
            throw new IllegalArgumentException( "Index path should be a directory" );
        }
        this.indexRootPath = indexRootPath;
    }

    /**
     * Perform index migrations
     * @throws ExplicitIndexMigrationException in case of exception during index migration
     */
    public void upgradeIndexes() throws ExplicitIndexMigrationException
    {
        try
        {
            if ( !Files.exists( indexRootPath ) )
            {
                return;
            }
            monitor.starting( (int) Files.walk( indexRootPath ).count() );
            try ( Stream<Path> pathStream = Files.walk( indexRootPath );
                  IndexUpgraderWrapper lucene4Upgrader = createIndexUpgrader( getLucene4JarPaths() );
                  IndexUpgraderWrapper lucene5Upgrader = createIndexUpgrader( getLucene5JarPaths() ) )
            {
                List<Path> indexPaths = pathStream.filter( getIndexPathFilter() ).collect( Collectors.toList() );
                for ( Path indexPath : indexPaths )
                {
                    try
                    {
                        lucene4Upgrader.upgradeIndex( indexPath );
                        lucene5Upgrader.upgradeIndex( indexPath );
                        monitor.migrated( indexPath.toFile().getName() );
                    }
                    catch ( Throwable e )
                    {
                        throw new ExplicitIndexMigrationException( indexPath.getFileName().toString(),
                                "Migration of explicit index at path:" + indexPath + " failed.", e );
                    }
                }
            }
        }
        catch ( ExplicitIndexMigrationException ime )
        {
            throw ime;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Lucene explicit indexes migration failed.", e );
        }
    }

    IndexUpgraderWrapper createIndexUpgrader( String[] jars )
    {
        return new IndexUpgraderWrapper( JarLoaderSupplier.of( jars ) );
    }

    private static String[] getLucene5JarPaths()
    {
        return getJarsFullPaths( LUCENE5_CORE_JAR_NAME, LUCENE5_BACKWARD_CODECS_NAME );
    }

    private static String[] getLucene4JarPaths()
    {
        return getJarsFullPaths( LUCENE4_CORE_JAR_NAME );
    }

    private static String[] getJarsFullPaths( String... jars )
    {
        return Stream.of( jars )
                .map( LuceneExplicitIndexUpgrader::getJarPath ).toArray( String[]::new );
    }

    private static String getJarPath( String library )
    {
        return LIBRARY_DIRECTORY + RESOURCE_SEPARATOR + library;
    }

    private static Predicate<Path> getIndexPathFilter()
    {
        return path ->
        {
            try
            {
                return Files.isDirectory( path ) && isIndexDirectory( path );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private static boolean isIndexDirectory( Path path ) throws IOException
    {
        try ( Stream<Path> pathStream = Files.list( path ) )
        {
            return pathStream.anyMatch( child -> child.getFileName().toString().startsWith( SEGMENTS_FILE_NAME_PREFIX ) );
        }
    }

}
