/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.counts.CountsTracker;

public enum StoreType
{
    NODE_LABEL( StoreFactory.NODE_LABELS_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicArrayStore( getStoreName(), IdType.NODE_LABELS,
                            GraphDatabaseSettings.label_block_size );
                }
            },
    NODE( StoreFactory.NODE_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createNodeStore( getStoreName() );
                }
            },
    PROPERTY_KEY_TOKEN_NAME( StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.PROPERTY_KEY_TOKEN_NAME,
                            TokenStore.NAME_STORE_BLOCK_SIZE );
                }
            },
    PROPERTY_KEY_TOKEN( StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyKeyTokenStore( getStoreName() );
                }
            },
    PROPERTY_STRING( StoreFactory.PROPERTY_STRINGS_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.STRING_BLOCK,
                            GraphDatabaseSettings.string_block_size );
                }
            },
    PROPERTY_ARRAY( StoreFactory.PROPERTY_ARRAYS_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicArrayStore( getStoreName(), IdType.ARRAY_BLOCK,
                            GraphDatabaseSettings.array_block_size );
                }
            },
    PROPERTY( StoreFactory.PROPERTY_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyStore( getStoreName() );
                }
            },
    RELATIONSHIP( StoreFactory.RELATIONSHIP_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipStore( getStoreName() );
                }
            },
    RELATIONSHIP_TYPE_TOKEN_NAME( StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.RELATIONSHIP_TYPE_TOKEN_NAME,
                            TokenStore.NAME_STORE_BLOCK_SIZE );
                }
            },
    RELATIONSHIP_TYPE_TOKEN( StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipTypeTokenStore( getStoreName() );
                }
            },
    LABEL_TOKEN_NAME( StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.LABEL_TOKEN_NAME,
                            TokenStore.NAME_STORE_BLOCK_SIZE );
                }
            },
    LABEL_TOKEN( StoreFactory.LABEL_TOKEN_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createLabelTokenStore( getStoreName() );
                }
            },
    SCHEMA( StoreFactory.SCHEMA_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createSchemaStore( getStoreName() );
                }
            },
    RELATIONSHIP_GROUP( StoreFactory.RELATIONSHIP_GROUP_STORE_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipGroupStore( getStoreName() );
                }
            },
    COUNTS( StoreFactory.COUNTS_STORE, false )
            {
                @Override
                public CountsTracker open( final NeoStores neoStores )
                {
                    return neoStores.createCountStore( getStoreName() );
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
            },
    META_DATA( MetaDataStore.DEFAULT_NAME ) // Make sure this META store is last
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createMetadataStore();
                }
            };

    private final boolean recordStore;
    private final String storeName;

    StoreType( String storeName )
    {
        this( storeName, true );
    }

    StoreType( String storeName, boolean recordStore )
    {
        this.storeName = storeName;
        this.recordStore = recordStore;
    }

    abstract Object open( NeoStores neoStores );

    public boolean isRecordStore()
    {
        return recordStore;
    }

    public String getStoreName()
    {
        return storeName;
    }

    void close( NeoStores me, Object object )
    {
        ((CommonAbstractStore) object).close();
    }

    /**
     * Determine type of a store base on a store file name.
     *
     * @param storeFileName - name of the store to map
     * @return store type of specified file
     * @throws IllegalStateException if can't determine store type for specified file
     */
    public static StoreType typeOf( String storeFileName )
    {
        StoreType[] values = StoreType.values();
        for ( StoreType value : values )
        {
            if ( storeFileName.equals( MetaDataStore.DEFAULT_NAME + value.getStoreName() ) )
            {
                return value;
            }
        }
        throw new IllegalArgumentException( "No enum constant for " + storeFileName + " file." );
    }
}
