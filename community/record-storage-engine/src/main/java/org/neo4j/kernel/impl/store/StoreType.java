/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.layout.recordstorage.RecordDatabaseFile;

public enum StoreType {
    NODE_LABEL(RecordDatabaseFile.NODE_LABEL_STORE, RecordIdType.NODE_LABELS) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createNodeLabelStore();
        }
    },
    NODE(RecordDatabaseFile.NODE_STORE, RecordIdType.NODE) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createNodeStore();
        }
    },
    PROPERTY_KEY_TOKEN_NAME(RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE, RecordIdType.PROPERTY_KEY_TOKEN_NAME) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createPropertyKeyTokenNamesStore();
        }
    },
    PROPERTY_KEY_TOKEN(RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE, SchemaIdType.PROPERTY_KEY_TOKEN) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createPropertyKeyTokenStore();
        }
    },
    PROPERTY_STRING(RecordDatabaseFile.PROPERTY_STRING_STORE, RecordIdType.STRING_BLOCK) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createPropertyStringStore();
        }
    },
    PROPERTY_ARRAY(RecordDatabaseFile.PROPERTY_ARRAY_STORE, RecordIdType.ARRAY_BLOCK) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createPropertyArrayStore();
        }
    },
    PROPERTY(RecordDatabaseFile.PROPERTY_STORE, RecordIdType.PROPERTY) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createPropertyStore();
        }
    },
    RELATIONSHIP(RecordDatabaseFile.RELATIONSHIP_STORE, RecordIdType.RELATIONSHIP) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createRelationshipStore();
        }
    },
    RELATIONSHIP_TYPE_TOKEN_NAME(
            RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE, RecordIdType.RELATIONSHIP_TYPE_TOKEN_NAME) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createRelationshipTypeTokenNamesStore();
        }
    },
    RELATIONSHIP_TYPE_TOKEN(RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE, SchemaIdType.RELATIONSHIP_TYPE_TOKEN) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createRelationshipTypeTokenStore();
        }
    },
    LABEL_TOKEN_NAME(RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE, RecordIdType.LABEL_TOKEN_NAME) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createLabelTokenNamesStore();
        }
    },
    LABEL_TOKEN(RecordDatabaseFile.LABEL_TOKEN_STORE, SchemaIdType.LABEL_TOKEN) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createLabelTokenStore();
        }
    },
    SCHEMA(RecordDatabaseFile.SCHEMA_STORE, SchemaIdType.SCHEMA) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createSchemaStore();
        }
    },
    RELATIONSHIP_GROUP(RecordDatabaseFile.RELATIONSHIP_GROUP_STORE, RecordIdType.RELATIONSHIP_GROUP) {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createRelationshipGroupStore();
        }
    },
    META_DATA(RecordDatabaseFile.METADATA_STORE, RecordIdType.NODE) // Make sure this META store is last
    {
        @Override
        public CommonAbstractStore open(NeoStores neoStores) {
            return neoStores.createMetadataStore();
        }
    };

    private final RecordDatabaseFile databaseFile;
    private final IdType idType;

    StoreType(RecordDatabaseFile databaseFile, IdType idType) {
        this.databaseFile = databaseFile;
        this.idType = idType;
    }

    abstract CommonAbstractStore open(NeoStores neoStores);

    public RecordDatabaseFile getDatabaseFile() {
        return databaseFile;
    }

    public IdType getIdType() {
        return idType;
    }

    public static final StoreType[] STORE_TYPES = values();

    /**
     * Determine the type of store base on provided database file.
     *
     * @param file - database file to map
     * @return an {@link Optional} that wraps the matching store type of the specified file,
     * or {@link Optional#empty()} if the given file name does not match any store files.
     */
    public static Optional<StoreType> typeOf(RecordDatabaseFile file) {
        Objects.requireNonNull(file);
        return Arrays.stream(STORE_TYPES)
                .filter(type -> type.getDatabaseFile().equals(file))
                .findFirst();
    }
}
