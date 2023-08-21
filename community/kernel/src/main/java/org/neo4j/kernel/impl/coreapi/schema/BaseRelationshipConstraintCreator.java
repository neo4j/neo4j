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
package org.neo4j.kernel.impl.coreapi.schema;

import static org.neo4j.graphdb.schema.IndexSettingUtil.toIndexConfigFromIndexSettingObjectMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.PropertyType;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.util.VisibleForTesting;

public class BaseRelationshipConstraintCreator extends AbstractConstraintCreator implements ConstraintCreator {
    protected final RelationshipType type;

    BaseRelationshipConstraintCreator(
            InternalSchemaActions actions,
            String name,
            RelationshipType type,
            IndexType indexType,
            IndexConfig indexConfig) {
        super(actions, name, indexType, indexConfig);
        this.type = type;
    }

    @Override
    public ConstraintCreator assertPropertyIsUnique(String propertyKey) {
        return new RelationshipPropertyUniqueConstraintCreator(
                actions, name, type, List.of(propertyKey), indexType, indexConfig);
    }

    @Override
    public ConstraintCreator assertPropertyExists(String propertyKey) {
        return new RelationshipPropertyExistenceCreator(actions, name, type, propertyKey, indexType, indexConfig);
    }

    @Override
    public ConstraintCreator assertPropertyIsNodeKey(String propertyKey) {
        throw new UnsupportedOperationException("Node key constraints are not supported on relationships.");
    }

    @Override
    public ConstraintCreator assertPropertyIsRelationshipKey(String propertyKey) {
        return new RelationshipKeyConstraintCreator(actions, name, type, List.of(propertyKey), indexType, indexConfig);
    }

    @Override
    public ConstraintCreator assertPropertyHasType(String propertyKey, PropertyType... propertyType) {
        PropertyTypeSet propertyTypeSet = validatePropertyTypes(propertyType);
        return new RelationshipPropertyTypeConstraintCreator(
                actions, name, type, propertyKey, indexType, indexConfig, propertyTypeSet);
    }

    @VisibleForTesting
    public ConstraintCreator assertPropertyHasType(
            String propertyKey, SchemaValueType propertyType, SchemaValueType... propertyTypes) {
        var entries = new ArrayList<SchemaValueType>();
        entries.add(propertyType);
        entries.addAll(Arrays.asList(propertyTypes));
        return new RelationshipPropertyTypeConstraintCreator(
                actions, name, type, propertyKey, indexType, indexConfig, PropertyTypeSet.of(entries));
    }

    @Override
    public ConstraintCreator withName(String name) {
        return new BaseRelationshipConstraintCreator(actions, name, type, indexType, indexConfig);
    }

    @Override
    public ConstraintCreator withIndexType(IndexType indexType) {
        return new BaseRelationshipConstraintCreator(actions, name, type, indexType, indexConfig);
    }

    @Override
    public ConstraintCreator withIndexConfiguration(Map<IndexSetting, Object> indexConfiguration) {
        return new BaseRelationshipConstraintCreator(
                actions, name, type, indexType, toIndexConfigFromIndexSettingObjectMap(indexConfiguration));
    }
}
