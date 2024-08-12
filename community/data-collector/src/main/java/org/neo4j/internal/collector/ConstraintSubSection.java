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
package org.neo4j.internal.collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.RelationshipEndpointConstraintDescriptor;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;

final class ConstraintSubSection {
    private ConstraintSubSection() { // only static functionality
    }

    public static Map<String, Object> constraint(
            TokenNameLookup tokens, Anonymizer anonymizer, ConstraintDescriptor constraint) {
        Map<String, Object> data = new HashMap<>();

        EntityType entityType = constraint.schema().entityType();
        int entityTokenId = constraint.schema().getEntityTokenIds()[0];
        switch (entityType) {
            case NODE:
                String label = anonymizer.label(tokens.labelGetName(entityTokenId), entityTokenId);
                data.put("label", label);
                break;
            case RELATIONSHIP:
                String relationshipType =
                        anonymizer.relationshipType(tokens.relationshipTypeGetName(entityTokenId), entityTokenId);
                data.put("relationshipType", relationshipType);
                break;
            default:
        }

        List<String> properties = Arrays.stream(constraint.schema().getPropertyIds())
                .mapToObj(id -> anonymizer.propertyKey(tokens.propertyKeyGetName(id), id))
                .toList();
        data.put("properties", properties);

        switch (constraint.type()) {
            case EXISTS:
                data.put("type", "Existence constraint");
                break;
            case UNIQUE:
                data.put("type", "Uniqueness constraint");
                break;
            case UNIQUE_EXISTS:
                data.put("type", "Node Key");
                break;
            case PROPERTY_TYPE:
                data.put("type", "Property type constraint");
                TypeConstraintDescriptor typeConstraintDescriptor = constraint.asPropertyTypeConstraint();
                PropertyTypeSet propertyTypeSet = typeConstraintDescriptor.propertyType();
                List<String> propertyTypes =
                        propertyTypeSet.stream().map(SchemaValueType::serialize).toList();
                data.put("propertyTypes", propertyTypes);
                break;
            case ENDPOINT:
                RelationshipEndpointConstraintDescriptor endpointConstraintDescriptor =
                        constraint.asRelationshipEndpointConstraint();
                data.put("type", "Relationship endpoint constraint");
                data.put(
                        "endpointType",
                        endpointConstraintDescriptor.endpointType().name());
            default:
        }

        return data;
    }
}
