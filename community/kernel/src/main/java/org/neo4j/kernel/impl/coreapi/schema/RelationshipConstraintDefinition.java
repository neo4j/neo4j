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

import static java.util.Objects.requireNonNull;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl.relTypeNameList;

import java.util.Arrays;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.schema.ConstraintDescriptor;

abstract class RelationshipConstraintDefinition extends MultiPropertyConstraintDefinition {
    protected final RelationshipType relationshipType;

    RelationshipConstraintDefinition(
            InternalSchemaActions actions,
            ConstraintDescriptor constraint,
            RelationshipType relationshipType,
            String propertyKey) {
        super(actions, constraint, new String[] {propertyKey});
        this.relationshipType = requireNonNull(relationshipType);
    }

    RelationshipConstraintDefinition(
            InternalSchemaActions actions, ConstraintDescriptor constraint, IndexDefinition indexDefinition) {
        super(actions, constraint, indexDefinition);
        if (indexDefinition.isMultiTokenIndex()) {
            throw new IllegalArgumentException(
                    "Relationship constraints do not support multi-token definitions. That is, they cannot apply to more than one relationship type, "
                            + "but an attempt was made to create a relationship constraint on the following relationship types: "
                            + relTypeNameList(indexDefinition.getRelationshipTypes(), "", "."));
        }
        this.relationshipType = single(indexDefinition.getRelationshipTypes());
    }

    @Override
    public Label getLabel() {
        assertInUnterminatedTransaction();
        throw new IllegalStateException("Constraint is associated with relationships");
    }

    @Override
    public RelationshipType getRelationshipType() {
        assertInUnterminatedTransaction();
        return relationshipType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RelationshipConstraintDefinition that = (RelationshipConstraintDefinition) o;
        return relationshipType.name().equals(that.relationshipType.name())
                && Arrays.equals(propertyKeys, that.propertyKeys);
    }

    @Override
    public int hashCode() {
        return 31 * relationshipType.name().hashCode() + Arrays.hashCode(propertyKeys);
    }
}
