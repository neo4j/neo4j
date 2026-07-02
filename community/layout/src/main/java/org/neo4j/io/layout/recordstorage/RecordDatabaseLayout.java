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
package org.neo4j.io.layout.recordstorage;

import java.nio.file.Path;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.PlainDatabaseLayout;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class RecordDatabaseLayout extends PlainDatabaseLayout {
    private RecordDatabaseLayout(Neo4jLayout neo4jLayout, String databaseName) {
        super(neo4jLayout, databaseName);
    }

    public static RecordDatabaseLayout of(Neo4jLayout neo4jLayout, String databaseName) {
        return new RecordDatabaseLayout(neo4jLayout, databaseName);
    }

    public static RecordDatabaseLayout ofFlat(Path databaseDirectory) {
        Path canonical = FileUtils.getCanonicalFile(databaseDirectory);
        Path home = canonical.getParent();
        String dbName = canonical.getFileName().toString();
        return of(Neo4jLayout.ofFlat(home), dbName);
    }

    public static RecordDatabaseLayout cast(DatabaseLayout layout) {
        if (layout instanceof RecordDatabaseLayout rdl) {
            return rdl;
        }
        throw new IllegalArgumentException(layout.toString() + " does not describe a record storage database.");
    }

    public static RecordDatabaseLayout convert(DatabaseLayout layout) {
        return layout instanceof RecordDatabaseLayout rdl ? rdl : of(layout.getNeo4jLayout(), layout.getDatabaseName());
    }

    public static RecordDatabaseLayout of(Config config) {
        return of(Neo4jLayout.of(config), config.get(GraphDatabaseSettings.initial_default_database));
    }

    @Override
    public Path pathForExistsMarker() {
        return file(RecordDatabaseFile.EXISTS_MARKER).baseSegment();
    }

    @Override
    public StoreFile pathForStore(CommonDatabaseStores store) {
        return switch (store) {
            case NODE -> nodeStore();
            case COUNTS -> countStore();
            case LABEL_TOKENS -> labelTokenStore();
            case RELATIONSHIP_TYPE_TOKENS -> relationshipTypeTokenStore();
            case PROPERTY_KEY_TOKENS -> propertyKeyTokenStore();
            case SCHEMAS -> schemaStore();
            case INDEX_STATISTICS -> indexStatisticsStore();
            case METADATA -> metadataStore();
        };
    }

    public StoreFile countStore() {
        return file(RecordDatabaseFile.COUNTS_STORE);
    }

    public StoreFile relationshipGroupDegreesStore() {
        return file(RecordDatabaseFile.RELATIONSHIP_GROUP_DEGREES_STORE);
    }

    public StoreFile propertyStringStore() {
        return file(RecordDatabaseFile.PROPERTY_STRING_STORE);
    }

    public StoreFile relationshipStore() {
        return file(RecordDatabaseFile.RELATIONSHIP_STORE);
    }

    public StoreFile propertyStore() {
        return file(RecordDatabaseFile.PROPERTY_STORE);
    }

    public StoreFile nodeStore() {
        return file(RecordDatabaseFile.NODE_STORE);
    }

    public StoreFile nodeLabelStore() {
        return file(RecordDatabaseFile.NODE_LABEL_STORE);
    }

    public StoreFile propertyArrayStore() {
        return file(RecordDatabaseFile.PROPERTY_ARRAY_STORE);
    }

    public StoreFile propertyKeyTokenStore() {
        return file(RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE);
    }

    public StoreFile propertyKeyTokenNamesStore() {
        return file(RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE);
    }

    public StoreFile relationshipTypeTokenStore() {
        return file(RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE);
    }

    public StoreFile relationshipTypeTokenNamesStore() {
        return file(RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE);
    }

    public StoreFile labelTokenStore() {
        return file(RecordDatabaseFile.LABEL_TOKEN_STORE);
    }

    public StoreFile schemaStore() {
        return file(RecordDatabaseFile.SCHEMA_STORE);
    }

    public StoreFile relationshipGroupStore() {
        return file(RecordDatabaseFile.RELATIONSHIP_GROUP_STORE);
    }

    public StoreFile labelTokenNamesStore() {
        return file(RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE);
    }

    @Override
    public StoreFile indexStatisticsStore() {
        return file(RecordDatabaseFile.INDEX_STATISTICS_STORE);
    }

    @Override
    public StoreFile metadataStore() {
        return file(RecordDatabaseFile.METADATA_STORE);
    }

    public StoreFile idNodeStore() {
        return idFile(RecordDatabaseFile.NODE_STORE).get();
    }

    public StoreFile idNodeLabelStore() {
        return idFile(RecordDatabaseFile.NODE_LABEL_STORE).get();
    }

    public StoreFile idPropertyStore() {
        return idFile(RecordDatabaseFile.PROPERTY_STORE).get();
    }

    public StoreFile idPropertyKeyTokenStore() {
        return idFile(RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE).get();
    }

    public StoreFile idPropertyKeyTokenNamesStore() {
        return idFile(RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE).get();
    }

    public StoreFile idPropertyStringStore() {
        return idFile(RecordDatabaseFile.PROPERTY_STRING_STORE).get();
    }

    public StoreFile idPropertyArrayStore() {
        return idFile(RecordDatabaseFile.PROPERTY_ARRAY_STORE).get();
    }

    public StoreFile idRelationshipStore() {
        return idFile(RecordDatabaseFile.RELATIONSHIP_STORE).get();
    }

    public StoreFile idRelationshipGroupStore() {
        return idFile(RecordDatabaseFile.RELATIONSHIP_GROUP_STORE).get();
    }

    public StoreFile idRelationshipTypeTokenStore() {
        return idFile(RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE).get();
    }

    public StoreFile idRelationshipTypeTokenNamesStore() {
        return idFile(RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE).get();
    }

    public StoreFile idLabelTokenStore() {
        return idFile(RecordDatabaseFile.LABEL_TOKEN_STORE).get();
    }

    public StoreFile idLabelTokenNamesStore() {
        return idFile(RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE).get();
    }

    public StoreFile idSchemaStore() {
        return idFile(RecordDatabaseFile.SCHEMA_STORE).get();
    }

    @Override
    public boolean isRecoverableStore(DatabaseFile file) {
        assert file instanceof RecordDatabaseFile;
        return RecordDatabaseFile.RECOVERABLE_STORE_FILES.contains(file);
    }
}
