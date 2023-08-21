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
import static org.neo4j.kernel.impl.coreapi.schema.IndexCreatorImpl.copyAndAdd;

import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.PropertyType;
import org.neo4j.internal.schema.IndexConfig;

public class RelationshipPropertyUniqueConstraintCreator extends BaseRelationshipConstraintCreator {
    private final List<String> propertyKeys;

    RelationshipPropertyUniqueConstraintCreator(
            InternalSchemaActions internalCreator,
            String name,
            RelationshipType type,
            List<String> propertyKeys,
            IndexType indexType,
            IndexConfig indexConfig) {
        super(internalCreator, name, type, indexType, indexConfig);
        this.propertyKeys = propertyKeys;
    }

    @Override
    public final RelationshipPropertyUniqueConstraintCreator assertPropertyIsUnique(String propertyKey) {
        return new RelationshipPropertyUniqueConstraintCreator(
                actions, name, type, copyAndAdd(propertyKeys, propertyKey), indexType, indexConfig);
    }

    @Override
    public ConstraintCreator assertPropertyExists(String propertyKey) {
        List<String> keys = List.of(propertyKey);
        if (propertyKeys.equals(keys)) {
            return new RelationshipKeyConstraintCreator(actions, name, type, propertyKeys, indexType, indexConfig);
        }
        throw new UnsupportedOperationException(
                "You cannot create a constraint on two different sets of property keys: " + propertyKeys + " vs. "
                        + keys + ".");
    }

    @Override
    public ConstraintCreator assertPropertyIsRelationshipKey(String propertyKey) {
        return assertPropertyExists(propertyKey);
    }

    @Override
    public ConstraintCreator assertPropertyHasType(String propertyKey, PropertyType... propertyType) {
        throw new UnsupportedOperationException(
                "You cannot create a property type constraint together with other constraints.");
    }

    @Override
    public ConstraintCreator withName(String name) {
        return new RelationshipPropertyUniqueConstraintCreator(
                actions, name, type, propertyKeys, indexType, indexConfig);
    }

    @Override
    public ConstraintCreator withIndexType(IndexType indexType) {
        return new RelationshipPropertyUniqueConstraintCreator(
                actions, name, type, propertyKeys, indexType, indexConfig);
    }

    @Override
    public ConstraintCreator withIndexConfiguration(Map<IndexSetting, Object> indexConfiguration) {
        return new RelationshipPropertyUniqueConstraintCreator(
                actions,
                name,
                type,
                propertyKeys,
                indexType,
                toIndexConfigFromIndexSettingObjectMap(indexConfiguration));
    }

    @Override
    public final ConstraintDefinition create() {
        assertInUnterminatedTransaction();

        IndexDefinitionImpl definition = new IndexDefinitionImpl(
                actions, null, new RelationshipType[] {type}, propertyKeys.toArray(new String[0]), true);
        return actions.createRelationshipPropertyUniquenessConstraint(definition, name, indexType, indexConfig);
    }
}
