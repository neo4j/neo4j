/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.coreapi.schema;

import static java.lang.String.format;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.PropertyType;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.SchemaValueType;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;

public class RelationshipPropertyTypeConstraintDefinition extends RelationshipConstraintDefinition {
    public RelationshipPropertyTypeConstraintDefinition(
            InternalSchemaActions actions,
            ConstraintDescriptor constraint,
            RelationshipType relationshipType,
            String propertyKey) {
        super(actions, constraint, relationshipType, propertyKey);
    }

    @Override
    public ConstraintType getConstraintType() {
        assertInUnterminatedTransaction();
        return ConstraintType.RELATIONSHIP_PROPERTY_TYPE;
    }

    @Override
    public String toString() {
        // FIXME PTC guessed syntax - not implemented yet
        return format(
                "FOR ()-[%s:%s]-() REQUIRE %s IS :: %s",
                relationshipType.name().toLowerCase(),
                relationshipType.name(),
                propertyText(relationshipType.name().toLowerCase()),
                constraint.asPropertyTypeConstraint().propertyType().userDescription());
    }

    @Override
    public PropertyType[] getPropertyType() {
        assertInUnterminatedTransaction();
        PropertyTypeSet propertyTypeSet = constraint.asPropertyTypeConstraint().propertyType();
        return propertyTypeSet.stream().map(SchemaValueType::toPublicApi).toArray(PropertyType[]::new);
    }
}
