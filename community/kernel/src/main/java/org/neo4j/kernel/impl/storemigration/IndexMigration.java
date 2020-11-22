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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.CoordinateReferenceSystem;

enum IndexMigration
{
    LUCENE( "lucene", "1.0", asIndexProviderDescriptor( GraphDatabaseSettings.SchemaIndex.NATIVE30 ), true )
            {
                @Override
                Path[] providerRootDirectories( DatabaseLayout layout )
                {
                    Path luceneDir = directoryRootByProviderKey( layout.databaseDirectory(), providerKey );
                    Path lucene10Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return new Path[]{luceneDir, lucene10Dir};
                }

                @Override
                IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId,
                        PageCursorTracer cursorTracer, Log log ) throws IOException
                {
                    Path lucene10Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    Path spatialDirectory = getSpatialSubDirectory( indexId, lucene10Dir );
                    List<SpatialFile> spatialFiles = getSpatialFiles( fs, spatialDirectory );
                    return SpatialConfigExtractor.indexConfigFromSpatialFile( pageCache, spatialFiles, cursorTracer, log );
                }

            },
    NATIVE10( "lucene+native", "1.0", asIndexProviderDescriptor( GraphDatabaseSettings.SchemaIndex.NATIVE30 ), true )
            {
                @Override
                Path[] providerRootDirectories( DatabaseLayout layout )
                {
                    Path luceneNative10Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return new Path[]{luceneNative10Dir};
                }

                @Override
                IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId,
                        PageCursorTracer cursorTracer, Log log ) throws IOException
                {
                    Path providerRootDirectory = providerRootDirectories( layout )[0];
                    Path spatialDirectory = getSpatialSubDirectory( indexId, providerRootDirectory );
                    List<SpatialFile> spatialFiles = getSpatialFiles( fs, spatialDirectory );
                    return SpatialConfigExtractor.indexConfigFromSpatialFile( pageCache, spatialFiles, cursorTracer, log );
                }

            },
    NATIVE20( "lucene+native", "2.0", asIndexProviderDescriptor( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 ), true )
            {
                @Override
                Path[] providerRootDirectories( DatabaseLayout layout )
                {
                    Path luceneNative20Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return new Path[]{luceneNative20Dir};
                }

                @Override
                IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId,
                        PageCursorTracer cursorTracer, Log log ) throws IOException
                {
                    Path providerRootDirectory = providerRootDirectories( layout )[0];
                    Path spatialDirectory = getSpatialSubDirectory( indexId, providerRootDirectory );
                    List<SpatialFile> spatialFiles = getSpatialFiles( fs, spatialDirectory );
                    return SpatialConfigExtractor.indexConfigFromSpatialFile( pageCache, spatialFiles, cursorTracer, log );
                }

            },
    NATIVE_BTREE10( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerKey(), GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerVersion(),
            asIndexProviderDescriptor( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 ), false )
            {
                @Override
                Path[] providerRootDirectories( DatabaseLayout layout )
                {
                    Path nativeBtree10Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return new Path[]{nativeBtree10Dir};
                }

                @Override
                IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId,
                        PageCursorTracer cursorTracer, Log log ) throws IOException
                {
                    Path rootDir = providerRootDirectories( layout )[0];
                    Path genericFile = rootDir.resolve( String.valueOf( indexId ) ).resolve( "index-" + indexId );
                    return GenericConfigExtractor.indexConfigFromGenericFile( fs, pageCache, genericFile, cursorTracer, log );
                }

            },
    FULLTEXT10( "fulltext", "1.0", new IndexProviderDescriptor( "fulltext", "1.0" ), false )
            {
                @Override
                Path[] providerRootDirectories( DatabaseLayout layout )
                {
                    Path fulltext10Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return new Path[]{fulltext10Dir};
                }

                @Override
                IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId,
                        PageCursorTracer cursorTracer, Log log )
                {
                    // Fulltext index directory structure.
                    // └── schema
                    //    └── index
                    //        └── fulltext-1.0
                    //            └── 1
                    //                ├── fulltext-1.0
                    //                │   ├── 1
                    //                │   │   ├── segments_1
                    //                │   │   └── write.lock
                    //                │   ├── failure-message
                    //                │   └── fulltext-index.properties <- Fulltext index settings
                    //                └── fulltext-1.0.tx               <- Transaction folder
                    Path fulltext10Dir = providerRootDirectories( layout )[0];
                    Path directoryForIndex = fulltext10Dir.resolve( String.valueOf( indexId ) );
                    Path fulltextIndexDirectory = directoryBySubProvider( directoryForIndex, providerKey, providerVersion );
                    return FulltextConfigExtractor.indexConfigFromFulltextDirectory( fs, fulltextIndexDirectory );
                }

            };

    private static final Pattern CRS_FILE_PATTERN = Pattern.compile( "(\\d+)-(\\d+)" );
    private static final String SPATIAL_DIRECTORY_NAME = "spatial-1.0";

    final String providerKey;
    final String providerVersion;
    final IndexProviderDescriptor desiredAlternativeProvider;
    private final boolean retired;

    IndexMigration( String providerKey, String providerVersion, IndexProviderDescriptor desiredAlternativeProvider, boolean retired )
    {
        this.providerKey = providerKey;
        this.providerVersion = providerVersion;
        this.desiredAlternativeProvider = desiredAlternativeProvider;
        this.retired = retired;
    }

    abstract Path[] providerRootDirectories( DatabaseLayout layout );

    abstract IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId,
            PageCursorTracer cursorTracer, Log log ) throws IOException;

    /**
     * Returns the base schema index directory, i.e.
     *
     * <pre>
     * &lt;db&gt;/schema/index/
     * </pre>
     *
     * @param databaseStoreDir database store directory, i.e. {@code db} in the example above, where e.g. {@code nodestore} lives.
     * @return the base directory of schema indexing.
     */
    private static Path baseSchemaIndexFolder( Path databaseStoreDir )
    {
        return databaseStoreDir.resolve( "schema" ).resolve( "index" );
    }

    /**
     * @param databaseStoreDir store directory of database, i.e. {@code db} in the example above.
     * @return The index provider root directory
     */
    private static Path directoryRootByProviderKey( Path databaseStoreDir, String providerKey )
    {
        return baseSchemaIndexFolder( databaseStoreDir ).resolve( fileNameFriendly( providerKey ) );
    }

    /**
     * @param databaseStoreDir store directory of database, i.e. {@code db} in the example above.
     * @return The index provider root directory
     */
    private static Path directoryRootByProviderKeyAndVersion( Path databaseStoreDir, String providerKey, String providerVersion )
    {
        return baseSchemaIndexFolder( databaseStoreDir ).resolve( fileNameFriendly( providerKey + "-" + providerVersion ) );
    }

    private static Path directoryBySubProvider( Path parentProviderDir, String providerKey, String providerVersion )
    {
        return parentProviderDir.resolve( fileNameFriendly( providerKey + "-" + providerVersion ) );
    }

    private static String fileNameFriendly( String name )
    {
        return name.replaceAll( "\\+", "_" );
    }

    public static IndexMigration migrationFromOldProvider( String providerKey, String providerVersion )
    {
        for ( IndexMigration provider : values() )
        {
            if ( provider.providerKey.equals( providerKey ) && provider.providerVersion.equals( providerVersion ) )
            {
                return provider;
            }
        }
        throw new IllegalArgumentException( "Can not find old index provider " + providerKey + "-" + providerVersion );
    }

    public static IndexMigration[] retired()
    {
        return Arrays.stream( IndexMigration.values() )
                .filter( p -> p.retired )
                .toArray( IndexMigration[]::new );
    }

    public static IndexMigration[] nonRetired()
    {
        return Arrays.stream( IndexMigration.values() )
                .filter( p -> !p.retired )
                .toArray( IndexMigration[]::new );
    }

    private static Path getSpatialSubDirectory( long indexId, Path baseProviderDir )
    {
        return baseProviderDir.resolve( String.valueOf( indexId ) ).resolve( SPATIAL_DIRECTORY_NAME );
    }

    public static List<SpatialFile> getSpatialFiles( FileSystemAbstraction fs, Path spatialDirectory )
    {
        List<SpatialFile> spatialFiles = new ArrayList<>();
        Path[] files = fs.listFiles( spatialDirectory );
        if ( files != null )
        {
            for ( Path file : files )
            {
                String name = file.getFileName().toString();
                Matcher matcher = CRS_FILE_PATTERN.matcher( name );
                if ( matcher.matches() )
                {
                    int tableId = Integer.parseInt( matcher.group( 1 ) );
                    int code = Integer.parseInt( matcher.group( 2 ) );
                    CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( tableId, code );
                    spatialFiles.add( new SpatialFile( crs, file ) );
                }
            }
        }
        return spatialFiles;
    }

    private static IndexProviderDescriptor asIndexProviderDescriptor( GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        return new IndexProviderDescriptor( schemaIndex.providerKey(), schemaIndex.providerVersion() );
    }
}
