/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.storemigration.StoreFile;

public enum StoreType
{
    NODE_LABEL( StoreFile.NODE_LABEL_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicArrayStore( getStoreName(), IdType.NODE_LABELS,
                            GraphDatabaseSettings.label_block_size );
                }
            },
    NODE( StoreFile.NODE_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createNodeStore( getStoreName() );
                }
            },
    PROPERTY_KEY_TOKEN_NAME( StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.PROPERTY_KEY_TOKEN_NAME,
                            TokenStore.NAME_STORE_BLOCK_SIZE );
                }
            },
    PROPERTY_KEY_TOKEN( StoreFile.PROPERTY_KEY_TOKEN_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyKeyTokenStore( getStoreName() );
                }
            },
    PROPERTY_STRING( StoreFile.PROPERTY_STRING_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.STRING_BLOCK,
                            GraphDatabaseSettings.string_block_size );
                }
            },
    PROPERTY_ARRAY( StoreFile.PROPERTY_ARRAY_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicArrayStore( getStoreName(), IdType.ARRAY_BLOCK,
                            GraphDatabaseSettings.array_block_size );
                }
            },
    PROPERTY( StoreFile.PROPERTY_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyStore( getStoreName() );
                }
            },
    RELATIONSHIP( StoreFile.RELATIONSHIP_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipStore( getStoreName() );
                }
            },
    RELATIONSHIP_TYPE_TOKEN_NAME( StoreFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.RELATIONSHIP_TYPE_TOKEN_NAME,
                            TokenStore.NAME_STORE_BLOCK_SIZE );
                }
            },
    RELATIONSHIP_TYPE_TOKEN( StoreFile.RELATIONSHIP_TYPE_TOKEN_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipTypeTokenStore( getStoreName() );
                }
            },
    LABEL_TOKEN_NAME( StoreFile.LABEL_TOKEN_NAMES_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createDynamicStringStore( getStoreName(), IdType.LABEL_TOKEN_NAME,
                            TokenStore.NAME_STORE_BLOCK_SIZE );
                }
            },
    LABEL_TOKEN( StoreFile.LABEL_TOKEN_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createLabelTokenStore( getStoreName() );
                }
            },
    SCHEMA( StoreFile.SCHEMA_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createSchemaStore( getStoreName() );
                }
            },
    RELATIONSHIP_GROUP( StoreFile.RELATIONSHIP_GROUP_STORE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipGroupStore( getStoreName() );
                }
            },
    COUNTS( null, false )
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
            },
    META_DATA( StoreFile.NEO_STORE ) // Make sure this META store is last
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createMetadataStore();
                }
            };

    private final boolean recordStore;
    private final StoreFile storeFile;

    StoreType( StoreFile storeFile )
    {
        this( storeFile, true );
    }

    StoreType( StoreFile storeFile, boolean recordStore )
    {
        this.storeFile = storeFile;
        this.recordStore = recordStore;
    }

    abstract Object open( NeoStores neoStores );

    public boolean isRecordStore()
    {
        return recordStore;
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
     * @param storeFileName - name of the store to map
     * @return store type of specified file
     * @throws IllegalStateException if can't determine store type for specified file
     */
    public static StoreType typeOf( String storeFileName )
    {
        StoreType[] values = StoreType.values();
        for ( StoreType value : values )
        {
            if ( value.getStoreName().equals( storeFileName ) ||
                    storeFileName.equals( MetaDataStore.DEFAULT_NAME + value.getStoreName() ) )
            {
                return value;
            }
        }
        throw new IllegalArgumentException( "No enum constant for " + storeFileName + " file." );
    }
}
