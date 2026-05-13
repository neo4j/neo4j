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
package org.neo4j.kernel.api.exceptions.schema;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VALIDATION;
import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VERIFICATION;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.internal.schema.constraints.TypeRepresentation;
import org.neo4j.values.storable.Value;

public class PropertyTypeException extends ConstraintValidationException {
    private final long entityId;
    private final TypeConstraintDescriptor descriptor;
    private final Value value;

    private final Phase phase;

    private PropertyTypeException(
            ErrorGqlStatusObject gqlStatusObject,
            TypeConstraintDescriptor descriptor,
            Phase phase,
            long entityId,
            TokenNameLookup tokenNameLookup,
            Value value) {
        super(
                gqlStatusObject,
                descriptor,
                phase,
                format(descriptor.schema().entityType() == EntityType.NODE ? "Node(%s)" : "Relationship(%s)", entityId),
                tokenNameLookup);

        this.entityId = entityId;
        this.descriptor = descriptor;
        this.value = value;
        this.phase = phase;
    }

    // KNL-058 and KNL-059
    public static PropertyTypeException propertyTypeVerificationFailed(
            TypeConstraintDescriptor descriptor, long entityId, TokenNameLookup tokenNameLookup, Value value) {
        return getPropertyTypeException(descriptor, entityId, tokenNameLookup, value, VERIFICATION);
    }

    // KNL-060 and KNL-061
    public static PropertyTypeException propertyTypeValidationFailed(
            TypeConstraintDescriptor descriptor, long entityId, TokenNameLookup tokenNameLookup, Value value) {
        return getPropertyTypeException(descriptor, entityId, tokenNameLookup, value, VALIDATION);
    }

    private static PropertyTypeException getPropertyTypeException(
            TypeConstraintDescriptor descriptor,
            long entityId,
            TokenNameLookup tokenNameLookup,
            Value value,
            Phase phase) {
        SchemaDescriptor schema = descriptor.schema();
        boolean isNode = schema.entityType() == EntityType.NODE;
        ErrorGqlStatusObject gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N78)
                .withParam(GqlParams.StringParam.entityType, isNode ? "NODE" : "RELATIONSHIP")
                .withParam(GqlParams.NumberParam.entityId, entityId)
                .withParam(GqlParams.StringParam.tokenType, isNode ? "label" : "type")
                .withParam(
                        GqlParams.StringParam.token,
                        isNode
                                ? tokenNameLookup.labelGetName(schema.getLabelId())
                                : tokenNameLookup.relationshipTypeGetName(schema.getRelTypeId()))
                .withParam(GqlParams.StringParam.propKey, tokenNameLookup.propertyKeyGetName(schema.getPropertyId()))
                .withParam(
                        GqlParams.StringParam.valueType,
                        descriptor.propertyType().userDescription())
                .build();
        return new PropertyTypeException(gql, descriptor, phase, entityId, tokenNameLookup, value);
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        SchemaDescriptor schema = descriptor.schema();
        EntityType entityType = schema.entityType();
        String entityString = entityType == EntityType.NODE ? "Node" : "Relationship";
        String entityTokenType = entityType == EntityType.NODE ? "label" : "type";
        String entityToken = entityType == EntityType.NODE
                ? tokenNameLookup.labelGetName(schema.getLabelId())
                : tokenNameLookup.relationshipTypeGetName(schema.getRelTypeId());
        String propertyKey = tokenNameLookup.propertyKeyGetName(schema.getPropertyId());

        return switch (phase) {
            case VERIFICATION ->
                format(
                        "%s(%s) property `%s` is of type `%s`.",
                        entityString,
                        entityId,
                        propertyKey,
                        TypeRepresentation.infer(value).userDescription());
            case VALIDATION ->
                format(
                        "%s(%s) with %s `%s` required the property `%s` to be of type `%s`, but was of type `%s`.",
                        entityString,
                        entityId,
                        entityTokenType,
                        entityToken,
                        propertyKey,
                        descriptor.propertyType().userDescription(),
                        TypeRepresentation.infer(value).userDescription());
        };
    }
}
