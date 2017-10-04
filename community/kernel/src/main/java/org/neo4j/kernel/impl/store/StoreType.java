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
package org.neo4j.kernel.impl.store;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.id.IdType;

public enum StoreType
{
    NODE_LABEL( StoreFile.NODE_LABEL_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicArrayStore( getStoreName(), IdType.NODE_LABELS,
                            GraphDatabaseSettings.label_block_size );
                }
            },
    NODE( StoreFile.NODE_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createNodeStore( getStoreName() );
                }
            },
    PROPERTY_KEY_TOKEN_NAME( StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.PROPERTY_KEY_TOKEN_NAME,
                            TokenStore.NAME_STORE_BLOCK_SIZE );
                }
            },
    PROPERTY_KEY_TOKEN( StoreFile.PROPERTY_KEY_TOKEN_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyKeyTokenStore( getStoreName() );
                }
            },
    PROPERTY_STRING( StoreFile.PROPERTY_STRING_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.STRING_BLOCK,
                            GraphDatabaseSettings.string_block_size );
                }
            },
    PROPERTY_ARRAY( StoreFile.PROPERTY_ARRAY_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicArrayStore( getStoreName(), IdType.ARRAY_BLOCK,
                            GraphDatabaseSettings.array_block_size );
                }
            },
    PROPERTY( StoreFile.PROPERTY_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyStore( getStoreName() );
                }
            },
    RELATIONSHIP( StoreFile.RELATIONSHIP_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipStore( getStoreName() );
                }
            },
    RELATIONSHIP_TYPE_TOKEN_NAME( StoreFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.RELATIONSHIP_TYPE_TOKEN_NAME,
                            TokenStore.NAME_STORE_BLOCK_SIZE );
                }
            },
    RELATIONSHIP_TYPE_TOKEN( StoreFile.RELATIONSHIP_TYPE_TOKEN_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipTypeTokenStore( getStoreName() );
                }
            },
    LABEL_TOKEN_NAME( StoreFile.LABEL_TOKEN_NAMES_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.LABEL_TOKEN_NAME,
                            TokenStore.NAME_STORE_BLOCK_SIZE );
                }
            },
    LABEL_TOKEN( StoreFile.LABEL_TOKEN_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createLabelTokenStore( getStoreName() );
                }
            },
    SCHEMA( StoreFile.SCHEMA_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createSchemaStore( getStoreName() );
                }
            },
    RELATIONSHIP_GROUP( StoreFile.RELATIONSHIP_GROUP_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipGroupStore( getStoreName() );
                }
            },
    COUNTS( null, false, false )
            {
                @Override
                public CountsTracker open( final NeoStores neoStores )
                {
                    return neoStores.createCountStore( StoreFactory.COUNTS_STORE );
                }

                @Override
                void close( NeoStores me, Object object )
                {
                    try
                    {
                        ((CountsTracker) object).shutdown();
                    }
                    catch ( IOException e )
                    {
                        throw new UnderlyingStorageException( e );
                    }
                }

                @Override
                public String getStoreName()
                {
                    return StoreFactory.COUNTS_STORE;
                }

                @Override
                protected boolean isStoreFile( String fileName )
                {
                    return matchStoreName( fileName, getStoreName() + CountsTracker.RIGHT ) ||
                           matchStoreName( fileName, getStoreName() + CountsTracker.LEFT );
                }
            },
    META_DATA( StoreFile.NEO_STORE, true, true ) // Make sure this META store is last
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createMetadataStore();
                }
            };

    private final boolean recordStore;
    private final boolean limitedIdStore;
    private final StoreFile storeFile;

    StoreType( StoreFile storeFile, boolean recordStore, boolean limitedIdStore )
    {
        this.storeFile = storeFile;
        this.recordStore = recordStore;
        this.limitedIdStore = limitedIdStore;
    }

    abstract Object open( NeoStores neoStores );

    public boolean isRecordStore()
    {
        return recordStore;
    }

    /**
     * @return {@code true} to signal that this store has a quite limited id space and is more of a meta data store.
     * Originally came about when adding transaction-local id batching, to avoid id generator batching on certain stores.
     */
    public boolean isLimitedIdStore()
    {
        return limitedIdStore;
    }

    public String getStoreName()
    {
        return storeFile.fileNamePart();
    }

    public StoreFile getStoreFile()
    {
        return storeFile;
    }

    void close( NeoStores me, Object object )
    {
        ((CommonAbstractStore) object).close();
    }

    /**
     * Determine type of a store base on a store file name.
     *
     * @param fileName - exact file name of the store to map
     * @return an {@link Optional} that wraps the matching store type of the specified file,
     * or {@link Optional#empty()} if the given file name does not match any store file name.
     */
    public static Optional<StoreType> typeOf( String fileName )
    {
        StoreType[] values = StoreType.values();
        for ( StoreType value : values )
        {
            if ( value.isStoreFile( fileName ) )
            {
                return Optional.of( value );
            }
        }
        return Optional.empty();
    }

    /**
     * Returns whether or not store file by given file name should be managed by the page cache.
     * Store files not managed by the page cache will end up on the otherwise specified {@link FileSystemAbstraction}.
     *
     * @param storeFileName file name of the store file to check.
     * @return Returns whether or not store file by given file name should be managed by the page cache.
     */
    public static boolean shouldBeManagedByPageCache( String storeFileName )
    {
        boolean isLabelScanStore = NativeLabelScanStore.FILE_NAME.equals( storeFileName );
        return isLabelScanStore || StoreType.typeOf( storeFileName ).map( StoreType::isRecordStore ).orElse( false );
    }

    protected boolean isStoreFile( String fileName )
    {
        return matchStoreName( fileName, getStoreName() );
    }

    /**
     * Helper method for {@link #isStoreFile(String)}. Given a file name and store name, see if they match.
     *
     * @param fileName File name to match.
     * @param storeName Name of store to match with.
     * @return {@code true} if file name match with store name, otherwise false.
     */
    protected boolean matchStoreName( String fileName, String storeName )
    {
        return fileName.equals( MetaDataStore.DEFAULT_NAME + storeName );
    }
}
