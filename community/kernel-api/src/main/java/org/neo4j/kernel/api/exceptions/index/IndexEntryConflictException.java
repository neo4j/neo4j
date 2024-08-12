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
package org.neo4j.kernel.api.exceptions.index;

import static java.lang.String.format;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_ENTITY;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

public class IndexEntryConflictException extends KernelException {
    private final SchemaDescriptor schemaDescriptor;
    private final ValueTuple propertyValues;
    private final long addedEntityId;
    private final long existingEntityId;

    public IndexEntryConflictException(
            SchemaDescriptor schemaDescriptor, long existingEntityId, long addedEntityId, Value... propertyValue) {
        this(schemaDescriptor, existingEntityId, addedEntityId, ValueTuple.of(propertyValue));
    }

    public IndexEntryConflictException(
            ErrorGqlStatusObject gqlStatusObject,
            SchemaDescriptor schemaDescriptor,
            long existingEntityId,
            long addedEntityId,
            Value... propertyValue) {
        this(gqlStatusObject, schemaDescriptor, existingEntityId, addedEntityId, ValueTuple.of(propertyValue));
    }

    public IndexEntryConflictException(
            SchemaDescriptor schemaDescriptor, long existingEntityId, long addedEntityId, ValueTuple propertyValues) {
        super(
                Status.Schema.ConstraintViolation,
                buildErrorMessage(
                        SchemaUserDescription.TOKEN_ID_NAME_LOOKUP,
                        schemaDescriptor,
                        propertyValues,
                        addedEntityId,
                        existingEntityId));
        this.schemaDescriptor = schemaDescriptor;
        this.existingEntityId = existingEntityId;
        this.addedEntityId = addedEntityId;
        this.propertyValues = propertyValues;
    }

    public IndexEntryConflictException(
            ErrorGqlStatusObject gqlStatusObject,
            SchemaDescriptor schemaDescriptor,
            long existingEntityId,
            long addedEntityId,
            ValueTuple propertyValues) {
        super(
                gqlStatusObject,
                Status.Schema.ConstraintViolation,
                buildErrorMessage(
                        SchemaUserDescription.TOKEN_ID_NAME_LOOKUP,
                        schemaDescriptor,
                        propertyValues,
                        addedEntityId,
                        existingEntityId));

        this.schemaDescriptor = schemaDescriptor;
        this.existingEntityId = existingEntityId;
        this.addedEntityId = addedEntityId;
        this.propertyValues = propertyValues;
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        return buildErrorMessage(tokenNameLookup, schemaDescriptor, propertyValues, addedEntityId, existingEntityId);
    }

    private static String buildErrorMessage(
            TokenNameLookup tokenNameLookup,
            SchemaDescriptor schemaDescriptor,
            ValueTuple propertyValues,
            long addedEntityId,
            long existingEntityId) {
        assert schemaDescriptor.getPropertyIds().length == propertyValues.size();

        String entityName;
        String tokenName;
        if (schemaDescriptor.entityType() == NODE) {
            entityName = "Node";
            tokenName = Arrays.stream(schemaDescriptor.getEntityTokenIds())
                    .mapToObj(tokenNameLookup::labelGetName)
                    .collect(Collectors.joining("`, `", "label `", "`"));
        } else {
            entityName = "Relationship";
            tokenName = Arrays.stream(schemaDescriptor.getEntityTokenIds())
                    .mapToObj(tokenNameLookup::relationshipTypeGetName)
                    .collect(Collectors.joining("`, `", "type `", "`"));
        }

        if (existingEntityId == NO_SUCH_ENTITY && addedEntityId == NO_SUCH_ENTITY) {
            return format(
                    "A %s already exists with %s and %s",
                    entityName,
                    tokenName,
                    propertyString(tokenNameLookup, schemaDescriptor.getPropertyIds(), propertyValues));
        } else if (addedEntityId == NO_SUCH_ENTITY) {
            return format(
                    "%s(%d) already exists with %s and %s",
                    entityName,
                    existingEntityId,
                    tokenName,
                    propertyString(tokenNameLookup, schemaDescriptor.getPropertyIds(), propertyValues));
        } else if (existingEntityId == NO_SUCH_ENTITY) {
            return format(
                    "Both another %s and %s(%d) have the %s and %s",
                    entityName,
                    entityName,
                    addedEntityId,
                    tokenName,
                    propertyString(tokenNameLookup, schemaDescriptor.getPropertyIds(), propertyValues));
        } else {
            return format(
                    "Both %s(%d) and %s(%d) have the %s and %s",
                    entityName,
                    existingEntityId,
                    entityName,
                    addedEntityId,
                    tokenName,
                    propertyString(tokenNameLookup, schemaDescriptor.getPropertyIds(), propertyValues));
        }
    }

    @VisibleForTesting
    public ValueTuple getPropertyValues() {
        return propertyValues;
    }

    @VisibleForTesting
    public long getAddedEntityId() {
        return addedEntityId;
    }

    @VisibleForTesting
    public long getExistingEntityId() {
        return existingEntityId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof IndexEntryConflictException that) {
            return addedEntityId == that.addedEntityId
                    && existingEntityId == that.existingEntityId
                    && Objects.equals(propertyValues, that.propertyValues);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(addedEntityId, existingEntityId, propertyValues);
    }

    @Override
    public String toString() {
        return "IndexEntryConflictException{" + "propertyValues="
                + propertyValues + ", addedEntityId="
                + addedEntityId + ", existingEntityId="
                + existingEntityId + '}';
    }

    private static String propertyString(
            TokenNameLookup tokenNameLookup, int[] propertyIds, ValueTuple propertyValues) {
        StringBuilder sb = new StringBuilder();
        String sep = propertyIds.length > 1 ? "properties " : "property ";
        for (int i = 0; i < propertyIds.length; i++) {
            sb.append(sep);
            sep = ", ";
            sb.append('`');
            sb.append(tokenNameLookup.propertyKeyGetName(propertyIds[i]));
            sb.append("` = ");
            sb.append(propertyValues.valueAt(i).prettyPrint());
        }
        return sb.toString();
    }
}
