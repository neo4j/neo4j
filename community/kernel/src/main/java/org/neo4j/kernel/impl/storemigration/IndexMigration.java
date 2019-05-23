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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;

import static org.neo4j.io.fs.FileUtils.path;

enum IndexMigration
{
    LUCENE( "lucene", "1.0", asIndexProviderDescriptor( GraphDatabaseSettings.SchemaIndex.NATIVE30 ), true )
            {
                @Override
                File[] providerRootDirectories( DatabaseLayout layout )
                {
                    File luceneDir = directoryRootByProviderKey( layout.databaseDirectory(), providerKey );
                    File lucene10Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return new File[]{luceneDir, lucene10Dir};
                }

                @Override
                IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId ) throws IOException
                {
                    File lucene10Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return SpatialConfigExtractor.indexConfigFromSpatialFile( fs, pageCache, lucene10Dir, indexId );
                }

            },
    NATIVE10( "lucene+native", "1.0", asIndexProviderDescriptor( GraphDatabaseSettings.SchemaIndex.NATIVE30 ), true )
            {
                @Override
                File[] providerRootDirectories( DatabaseLayout layout )
                {
                    File lucene_native10Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return new File[]{lucene_native10Dir};
                }

                @Override
                IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId ) throws IOException
                {
                    File providerRootDirectory = providerRootDirectories( layout )[0];
                    return SpatialConfigExtractor.indexConfigFromSpatialFile( fs, pageCache, providerRootDirectory, indexId );
                }

            },
    NATIVE20( "lucene+native", "2.0", asIndexProviderDescriptor( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 ), true )
            {
                @Override
                File[] providerRootDirectories( DatabaseLayout layout )
                {
                    File lucene_native20Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return new File[]{lucene_native20Dir};
                }

                @Override
                IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId ) throws IOException
                {
                    File providerRootDirectory = providerRootDirectories( layout )[0];
                    return SpatialConfigExtractor.indexConfigFromSpatialFile( fs, pageCache, providerRootDirectory, indexId );
                }

            },
    NATIVE_BTREE10( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerKey(), GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerVersion(),
            asIndexProviderDescriptor( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 ), false )
            {
                @Override
                File[] providerRootDirectories( DatabaseLayout layout )
                {
                    File native_btree10Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return new File[]{native_btree10Dir};
                }

                @Override
                IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId ) throws IOException
                {
                    File rootDir = providerRootDirectories( layout )[0];
                    return GenericConfigExtractor.indexConfigFromGenericFile( pageCache, rootDir, indexId );
                }

            },
    FULLTEXT10( "fulltext", "1.0", new IndexProviderDescriptor( "fulltext", "1.0" ), false )
            {
                @Override
                File[] providerRootDirectories( DatabaseLayout layout )
                {
                    File fulltext10Dir = directoryRootByProviderKeyAndVersion( layout.databaseDirectory(), providerKey, providerVersion );
                    return new File[]{fulltext10Dir};
                }

                @Override
                IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId )
                {
                    // Fulltext index directory structure.
                    // └── schema
                    //    └── index
                    //        └── fulltext-1.0
                    //            └── 1
                    //                ├── fulltext-1.0
                    //                │   ├── 1
                    //                │   │   ├── segments_1
                    //                │   │   └── write.lock
                    //                │   ├── failure-message
                    //                │   └── fulltext-index.properties <- Fulltext index settings
                    //                └── fulltext-1.0.tx               <- Transaction folder
                    File fulltext10Dir = providerRootDirectories( layout )[0];
                    File directoryForIndex = path( fulltext10Dir, String.valueOf( indexId ) );
                    File fulltextIndexDirectory = directoryBySubProvider( directoryForIndex, providerKey, providerVersion );
                    return FulltextConfigExtractor.indexConfigFromFulltextDirectory( fs, fulltextIndexDirectory );
                }

            };

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

    abstract File[] providerRootDirectories( DatabaseLayout layout );

    abstract IndexConfig extractIndexConfig( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, long indexId ) throws IOException;

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
    private static File baseSchemaIndexFolder( File databaseStoreDir )
    {
        return path( databaseStoreDir, "schema", "index" );
    }

    /**
     * @param databaseStoreDir store directory of database, i.e. {@code db} in the example above.
     * @return The index provider root directory
     */
    private static File directoryRootByProviderKey( File databaseStoreDir, String providerKey )
    {
        return path( baseSchemaIndexFolder( databaseStoreDir ), fileNameFriendly( providerKey ) );
    }

    /**
     * @param databaseStoreDir store directory of database, i.e. {@code db} in the example above.
     * @return The index provider root directory
     */
    private static File directoryRootByProviderKeyAndVersion( File databaseStoreDir, String providerKey, String providerVersion )
    {
        return path( baseSchemaIndexFolder( databaseStoreDir ), fileNameFriendly( providerKey + "-" + providerVersion ) );
    }

    private static File directoryBySubProvider( File parentProviderDir, String providerKey, String providerVersion )
    {
        return path( parentProviderDir, fileNameFriendly( providerKey + "-" + providerVersion ) );
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

    private static IndexProviderDescriptor asIndexProviderDescriptor( GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        return new IndexProviderDescriptor( schemaIndex.providerKey(), schemaIndex.providerVersion() );
    }
}
