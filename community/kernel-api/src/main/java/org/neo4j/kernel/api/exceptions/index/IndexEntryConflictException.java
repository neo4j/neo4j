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
import java.util.stream.Collectors;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

public class IndexEntryConflictException extends KernelException {
    private final ValueTuple propertyValues;
    private final long addedEntityId;
    private final long existingEntityId;

    /**
     * Make IOUtils happy
     */
    public IndexEntryConflictException(String message, Throwable cause) {
        super(Status.Schema.ConstraintViolation, message, cause);
        propertyValues = null;
        addedEntityId = -1;
        existingEntityId = -1;
    }

    public IndexEntryConflictException(
            EntityType entityType, long existingEntityId, long addedEntityId, Value... propertyValue) {
        this(entityType, existingEntityId, addedEntityId, ValueTuple.of(propertyValue));
    }

    public IndexEntryConflictException(
            EntityType entityType, long existingEntityId, long addedEntityId, ValueTuple propertyValues) {
        super(
                Status.Schema.ConstraintViolation,
                "Both %s %d and %s %d share the property value %s",
                entityType == NODE ? "node" : "relationship",
                existingEntityId,
                entityType == NODE ? "node" : "relationship",
                addedEntityId,
                propertyValues);
        this.existingEntityId = existingEntityId;
        this.addedEntityId = addedEntityId;
        this.propertyValues = propertyValues;
    }

    public String evidenceMessage(TokenNameLookup tokenNameLookup, SchemaDescriptor schema) {
        assert schema.getPropertyIds().length == propertyValues.size();

        String entityName;
        String tokenName;
        if (schema.entityType() == NODE) {
            entityName = "Node";
            tokenName = Arrays.stream(schema.getEntityTokenIds())
                    .mapToObj(tokenNameLookup::labelGetName)
                    .collect(Collectors.joining("`, `", "label `", "`"));
        } else {
            entityName = "Relationship";
            tokenName = Arrays.stream(schema.getEntityTokenIds())
                    .mapToObj(tokenNameLookup::relationshipTypeGetName)
                    .collect(Collectors.joining("`, `", "type `", "`"));
        }

        if (addedEntityId == NO_SUCH_ENTITY) {
            if (existingEntityId == NO_SUCH_ENTITY) {
                return format(
                        "A %s already exists with %s and %s",
                        entityName, tokenName, propertyString(tokenNameLookup, schema.getPropertyIds()));
            } else {
                return format(
                        "%s(%d) already exists with %s and %s",
                        entityName,
                        existingEntityId,
                        tokenName,
                        propertyString(tokenNameLookup, schema.getPropertyIds()));
            }
        } else {
            return format(
                    "Both %s(%d) and %s(%d) have the %s and %s",
                    entityName,
                    existingEntityId,
                    entityName,
                    addedEntityId,
                    tokenName,
                    propertyString(tokenNameLookup, schema.getPropertyIds()));
        }
    }

    public ValueTuple getPropertyValues() {
        return propertyValues;
    }

    public Value getSinglePropertyValue() {
        return propertyValues.getOnlyValue();
    }

    public long getAddedEntityId() {
        return addedEntityId;
    }

    public long getExistingEntityId() {
        return existingEntityId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexEntryConflictException that = (IndexEntryConflictException) o;

        return addedEntityId == that.addedEntityId
                && existingEntityId == that.existingEntityId
                && !(propertyValues != null
                        ? !propertyValues.equals(that.propertyValues)
                        : that.propertyValues != null);
    }

    @Override
    public int hashCode() {
        int result = propertyValues != null ? propertyValues.hashCode() : 0;
        result = 31 * result + (int) (addedEntityId ^ (addedEntityId >>> 32));
        result = 31 * result + (int) (existingEntityId ^ (existingEntityId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "IndexEntryConflictException{" + "propertyValues="
                + propertyValues + ", addedEntityId="
                + addedEntityId + ", existingEntityId="
                + existingEntityId + '}';
    }

    private String propertyString(TokenNameLookup tokenNameLookup, int[] propertyIds) {
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
