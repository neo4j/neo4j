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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.PropertyType;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.util.VisibleForTesting;

public class BaseNodeConstraintCreator extends AbstractConstraintCreator implements ConstraintCreator {
    protected final Label label;

    public BaseNodeConstraintCreator(
            InternalSchemaActions actions, String name, Label label, IndexType indexType, IndexConfig indexConfig) {
        super(actions, name, indexType, indexConfig);
        this.label = label;

        assertInUnterminatedTransaction();
    }

    @Override
    public ConstraintCreator assertPropertyIsUnique(String propertyKey) {
        return new NodePropertyUniqueConstraintCreator(
                actions, name, label, List.of(propertyKey), indexType, indexConfig);
    }

    @Override
    public ConstraintCreator assertPropertyExists(String propertyKey) {
        return new NodePropertyExistenceConstraintCreator(
                actions, name, label, List.of(propertyKey), indexType, indexConfig);
    }

    @Override
    public ConstraintCreator assertPropertyIsNodeKey(String propertyKey) {
        return new NodeKeyConstraintCreator(actions, name, label, List.of(propertyKey), indexType, indexConfig);
    }

    @Override
    public ConstraintCreator assertPropertyIsRelationshipKey(String propertyKey) {
        throw new UnsupportedOperationException("Relationship key constraints are not supported on nodes.");
    }

    @Override
    public ConstraintCreator assertPropertyHasType(String propertyKey, PropertyType... propertyType) {
        PropertyTypeSet propertyTypeSet = validatePropertyTypes(propertyType);
        return new NodePropertyTypeConstraintCreator(
                actions, name, label, propertyKey, indexType, indexConfig, propertyTypeSet);
    }

    @VisibleForTesting
    public ConstraintCreator assertPropertyHasType(
            String propertyKey, SchemaValueType propertyType, SchemaValueType... propertyTypes) {
        var entries = new ArrayList<SchemaValueType>();
        entries.add(propertyType);
        entries.addAll(Arrays.asList(propertyTypes));
        return new NodePropertyTypeConstraintCreator(
                actions, name, label, propertyKey, indexType, indexConfig, PropertyTypeSet.of(entries));
    }

    @Override
    public ConstraintCreator withName(String name) {
        return new BaseNodeConstraintCreator(actions, name, label, indexType, indexConfig);
    }

    @Override
    public ConstraintCreator withIndexType(IndexType indexType) {
        return new BaseNodeConstraintCreator(actions, name, label, indexType, indexConfig);
    }

    @Override
    public ConstraintCreator withIndexConfiguration(Map<IndexSetting, Object> indexConfiguration) {
        return new BaseNodeConstraintCreator(
                actions, name, label, indexType, toIndexConfigFromIndexSettingObjectMap(indexConfiguration));
    }
}
