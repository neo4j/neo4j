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
import java.util.stream.Stream;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.PlainDatabaseLayout;

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
        if (layout instanceof RecordDatabaseLayout) {
            return (RecordDatabaseLayout) layout;
        }
        throw new IllegalArgumentException(layout.toString() + " does not describe a record storage database.");
    }

    public static RecordDatabaseLayout convert(DatabaseLayout layout) {
        return layout instanceof RecordDatabaseLayout
                ? (RecordDatabaseLayout) layout
                : of(layout.getNeo4jLayout(), layout.getDatabaseName());
    }

    public static RecordDatabaseLayout of(Config config) {
        return of(Neo4jLayout.of(config), config.get(GraphDatabaseSettings.initial_default_database));
    }

    @Override
    public Path pathForExistsMarker() {
        return file(RecordDatabaseFile.EXISTS_MARKER.getName());
    }

    @Override
    public Path pathForStore(CommonDatabaseStores store) {
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

    public Path countStore() {
        return file(RecordDatabaseFile.COUNTS_STORE.getName());
    }

    public Path relationshipGroupDegreesStore() {
        return file(RecordDatabaseFile.RELATIONSHIP_GROUP_DEGREES_STORE.getName());
    }

    public Path propertyStringStore() {
        return file(RecordDatabaseFile.PROPERTY_STRING_STORE.getName());
    }

    public Path relationshipStore() {
        return file(RecordDatabaseFile.RELATIONSHIP_STORE.getName());
    }

    public Path propertyStore() {
        return file(RecordDatabaseFile.PROPERTY_STORE.getName());
    }

    public Path nodeStore() {
        return file(RecordDatabaseFile.NODE_STORE.getName());
    }

    public Path nodeLabelStore() {
        return file(RecordDatabaseFile.NODE_LABEL_STORE.getName());
    }

    public Path propertyArrayStore() {
        return file(RecordDatabaseFile.PROPERTY_ARRAY_STORE.getName());
    }

    public Path propertyKeyTokenStore() {
        return file(RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE.getName());
    }

    public Path propertyKeyTokenNamesStore() {
        return file(RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE.getName());
    }

    public Path relationshipTypeTokenStore() {
        return file(RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE.getName());
    }

    public Path relationshipTypeTokenNamesStore() {
        return file(RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE.getName());
    }

    public Path labelTokenStore() {
        return file(RecordDatabaseFile.LABEL_TOKEN_STORE.getName());
    }

    public Path schemaStore() {
        return file(RecordDatabaseFile.SCHEMA_STORE.getName());
    }

    public Path relationshipGroupStore() {
        return file(RecordDatabaseFile.RELATIONSHIP_GROUP_STORE.getName());
    }

    public Path labelTokenNamesStore() {
        return file(RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE.getName());
    }

    @Override
    public Path indexStatisticsStore() {
        return file(RecordDatabaseFile.INDEX_STATISTICS_STORE.getName());
    }

    @Override
    public Path metadataStore() {
        return file(RecordDatabaseFile.METADATA_STORE.getName());
    }

    public Path idNodeStore() {
        return idFile(RecordDatabaseFile.NODE_STORE.getName());
    }

    public Path idNodeLabelStore() {
        return idFile(RecordDatabaseFile.NODE_LABEL_STORE.getName());
    }

    public Path idPropertyStore() {
        return idFile(RecordDatabaseFile.PROPERTY_STORE.getName());
    }

    public Path idPropertyKeyTokenStore() {
        return idFile(RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE.getName());
    }

    public Path idPropertyKeyTokenNamesStore() {
        return idFile(RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE.getName());
    }

    public Path idPropertyStringStore() {
        return idFile(RecordDatabaseFile.PROPERTY_STRING_STORE.getName());
    }

    public Path idPropertyArrayStore() {
        return idFile(RecordDatabaseFile.PROPERTY_ARRAY_STORE.getName());
    }

    public Path idRelationshipStore() {
        return idFile(RecordDatabaseFile.RELATIONSHIP_STORE.getName());
    }

    public Path idRelationshipGroupStore() {
        return idFile(RecordDatabaseFile.RELATIONSHIP_GROUP_STORE.getName());
    }

    public Path idRelationshipTypeTokenStore() {
        return idFile(RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE.getName());
    }

    public Path idRelationshipTypeTokenNamesStore() {
        return idFile(RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE.getName());
    }

    public Path idLabelTokenStore() {
        return idFile(RecordDatabaseFile.LABEL_TOKEN_STORE.getName());
    }

    public Path idLabelTokenNamesStore() {
        return idFile(RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE.getName());
    }

    public Path idSchemaStore() {
        return idFile(RecordDatabaseFile.SCHEMA_STORE.getName());
    }

    @Override
    protected Stream<RecordDatabaseFile> databaseFiles() {
        return RecordDatabaseFile.STORE_FILES.stream();
    }

    @Override
    protected boolean isRecoverableStore(DatabaseFile file) {
        assert file instanceof RecordDatabaseFile;
        return RecordDatabaseFile.RECOVERABLE_STORE_FILES.contains(file);
    }
}
