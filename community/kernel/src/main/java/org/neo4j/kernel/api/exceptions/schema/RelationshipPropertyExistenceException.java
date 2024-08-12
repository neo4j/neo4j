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

import java.util.function.Function;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.token.api.TokenIdPrettyPrinter;

public class RelationshipPropertyExistenceException extends ConstraintValidationException {
    private final RelationTypeSchemaDescriptor schema;
    private final long relationshipId;

    public RelationshipPropertyExistenceException(
            RelationTypeSchemaDescriptor schema,
            Function<RelationTypeSchemaDescriptor, ConstraintDescriptor> constraintFunc,
            ConstraintValidationException.Phase phase,
            long relationshipId,
            TokenNameLookup tokenNameLookup) {
        super(constraintFunc.apply(schema), phase, format("Relationship(%s)", relationshipId), tokenNameLookup);
        this.schema = schema;
        this.relationshipId = relationshipId;
    }

    public RelationshipPropertyExistenceException(
            ErrorGqlStatusObject gqlStatusObject,
            RelationTypeSchemaDescriptor schema,
            Function<RelationTypeSchemaDescriptor, ConstraintDescriptor> constraintFunc,
            ConstraintValidationException.Phase phase,
            long relationshipId,
            TokenNameLookup tokenNameLookup) {
        super(
                gqlStatusObject,
                constraintFunc.apply(schema),
                phase,
                format("Relationship(%s)", relationshipId),
                tokenNameLookup);

        this.schema = schema;
        this.relationshipId = relationshipId;
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        boolean pluralProps = schema.getPropertyIds().length > 1;
        String propertyNoun = pluralProps ? "properties" : "property";
        String sep = pluralProps ? "" : "`";
        String props = pluralProps
                ? TokenIdPrettyPrinter.niceQuotedProperties(tokenNameLookup, schema.getPropertyIds())
                : tokenNameLookup.propertyKeyGetName(schema.getPropertyId());

        return format(
                "Relationship(%s) with type `%s` must have the %s %s%s%s",
                relationshipId,
                tokenNameLookup.relationshipTypeGetName(schema.getRelTypeId()),
                propertyNoun,
                sep,
                props,
                sep);
    }
}
