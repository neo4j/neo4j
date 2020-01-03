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
package org.neo4j.kernel.impl.store;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.kernel.impl.store.counts.CountsTracker;

public enum StoreType
{
    NODE_LABEL( DatabaseFile.NODE_LABEL_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createNodeLabelStore();
                }
            },
    NODE( DatabaseFile.NODE_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createNodeStore();
                }
            },
    PROPERTY_KEY_TOKEN_NAME( DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyKeyTokenNamesStore();
                }
            },
    PROPERTY_KEY_TOKEN( DatabaseFile.PROPERTY_KEY_TOKEN_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyKeyTokenStore();
                }
            },
    PROPERTY_STRING( DatabaseFile.PROPERTY_STRING_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyStringStore();
                }
            },
    PROPERTY_ARRAY( DatabaseFile.PROPERTY_ARRAY_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyArrayStore();
                }
            },
    PROPERTY( DatabaseFile.PROPERTY_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createPropertyStore();
                }
            },
    RELATIONSHIP( DatabaseFile.RELATIONSHIP_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipStore();
                }
            },
    RELATIONSHIP_TYPE_TOKEN_NAME( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipTypeTokenNamesStore();
                }
            },
    RELATIONSHIP_TYPE_TOKEN( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipTypeTokenStore();
                }
            },
    LABEL_TOKEN_NAME( DatabaseFile.LABEL_TOKEN_NAMES_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createLabelTokenNamesStore();
                }
            },
    LABEL_TOKEN( DatabaseFile.LABEL_TOKEN_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createLabelTokenStore();
                }
            },
    SCHEMA( DatabaseFile.SCHEMA_STORE, true, true )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createSchemaStore();
                }
            },
    RELATIONSHIP_GROUP( DatabaseFile.RELATIONSHIP_GROUP_STORE, true, false )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createRelationshipGroupStore();
                }
            },
    COUNTS( DatabaseFile.COUNTS_STORES, false, false )
            {
                @Override
                public CountsTracker open( NeoStores neoStores )
                {
                    return neoStores.createCountStore();
                }

                @Override
                void close( Object object )
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
    META_DATA( DatabaseFile.METADATA_STORE, true, true ) // Make sure this META store is last
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores )
                {
                    return neoStores.createMetadataStore();
                }
            };

    private final boolean recordStore;
    private final boolean limitedIdStore;
    private final DatabaseFile databaseFile;

    StoreType( DatabaseFile databaseFile, boolean recordStore, boolean limitedIdStore )
    {
        this.databaseFile = databaseFile;
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

    public DatabaseFile getDatabaseFile()
    {
        return databaseFile;
    }

    void close( Object object )
    {
        ((CommonAbstractStore) object).close();
    }

    /**
     * Determine type of a store base on provided database file.
     *
     * @param databaseFile - database file to map
     * @return an {@link Optional} that wraps the matching store type of the specified file,
     * or {@link Optional#empty()} if the given file name does not match any store files.
     */
    public static Optional<StoreType> typeOf( DatabaseFile databaseFile )
    {
        Objects.requireNonNull( databaseFile );
        StoreType[] values = StoreType.values();
        for ( StoreType value : values )
        {
            if ( value.getDatabaseFile().equals( databaseFile ) )
            {
                return Optional.of( value );
            }
        }
        return Optional.empty();
    }
}
