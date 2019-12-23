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

import java.util.Objects;
import java.util.Optional;

import org.neo4j.internal.id.IdType;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

public enum StoreType
{
    NODE_LABEL( DatabaseFile.NODE_LABEL_STORE, IdType.NODE_LABELS )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createNodeLabelStore( cursorTracer );
                }
            },
    NODE( DatabaseFile.NODE_STORE, IdType.NODE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createNodeStore( cursorTracer );
                }
            },
    PROPERTY_KEY_TOKEN_NAME( DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE, IdType.PROPERTY_KEY_TOKEN_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createPropertyKeyTokenNamesStore( cursorTracer );
                }
            },
    PROPERTY_KEY_TOKEN( DatabaseFile.PROPERTY_KEY_TOKEN_STORE, IdType.PROPERTY_KEY_TOKEN )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createPropertyKeyTokenStore( cursorTracer );
                }
            },
    PROPERTY_STRING( DatabaseFile.PROPERTY_STRING_STORE, IdType.STRING_BLOCK )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createPropertyStringStore( cursorTracer );
                }
            },
    PROPERTY_ARRAY( DatabaseFile.PROPERTY_ARRAY_STORE, IdType.ARRAY_BLOCK )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createPropertyArrayStore( cursorTracer );
                }
            },
    PROPERTY( DatabaseFile.PROPERTY_STORE, IdType.PROPERTY )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createPropertyStore( cursorTracer );
                }
            },
    RELATIONSHIP( DatabaseFile.RELATIONSHIP_STORE, IdType.RELATIONSHIP )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createRelationshipStore( cursorTracer );
                }
            },
    RELATIONSHIP_TYPE_TOKEN_NAME( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE, IdType.RELATIONSHIP_TYPE_TOKEN_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createRelationshipTypeTokenNamesStore( cursorTracer );
                }
            },
    RELATIONSHIP_TYPE_TOKEN( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE, IdType.RELATIONSHIP_TYPE_TOKEN )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createRelationshipTypeTokenStore( cursorTracer );
                }
            },
    LABEL_TOKEN_NAME( DatabaseFile.LABEL_TOKEN_NAMES_STORE, IdType.LABEL_TOKEN_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createLabelTokenNamesStore( cursorTracer );
                }
            },
    LABEL_TOKEN( DatabaseFile.LABEL_TOKEN_STORE, IdType.LABEL_TOKEN )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createLabelTokenStore( cursorTracer );
                }
            },
    SCHEMA( DatabaseFile.SCHEMA_STORE, IdType.SCHEMA )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createSchemaStore( cursorTracer );
                }
            },
    RELATIONSHIP_GROUP( DatabaseFile.RELATIONSHIP_GROUP_STORE, IdType.RELATIONSHIP_GROUP )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createRelationshipGroupStore( cursorTracer );
                }
            },
    META_DATA( DatabaseFile.METADATA_STORE, IdType.NEOSTORE_BLOCK ) // Make sure this META store is last
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createMetadataStore( cursorTracer );
                }
            };

    private final DatabaseFile databaseFile;
    private final IdType idType;

    StoreType( DatabaseFile databaseFile, IdType idType )
    {
        this.databaseFile = databaseFile;
        this.idType = idType;
    }

    abstract CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer );

    public DatabaseFile getDatabaseFile()
    {
        return databaseFile;
    }

    public IdType getIdType()
    {
        return idType;
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
