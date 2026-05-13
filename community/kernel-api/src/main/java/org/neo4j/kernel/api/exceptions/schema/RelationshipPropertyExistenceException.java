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
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.token.api.TokenIdPrettyPrinter;

public class RelationshipPropertyExistenceException extends ConstraintValidationException {
    private final RelationTypeSchemaDescriptor schema;
    private final long relationshipId;

    private RelationshipPropertyExistenceException(
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

    public static RelationshipPropertyExistenceException propertyPresenceViolation(
            RelationTypeSchemaDescriptor schema,
            TokenNameLookup tokenHolders,
            ConstraintDescriptor descriptor,
            ConstraintValidationException.Phase phase,
            long relationshipId) {
        // This might be a way to expose hidden properties to the user with roles with no access to those properties
        // TODO: check for user rights
        int[] propIds = schema.getPropertyIds();
        String[] propKeyNames = new String[propIds.length];
        for (int i = 0; i < propIds.length; i++) {
            propKeyNames[i] = tokenHolders.propertyKeyGetName(propIds[i]);
        }
        ErrorGqlStatusObject gql = GqlHelper.getGql22N77_relationships(
                relationshipId, tokenHolders.relationshipTypeGetName(schema.getRelTypeId()), propKeyNames);
        return new RelationshipPropertyExistenceException(
                gql, schema, ignored -> descriptor, phase, relationshipId, tokenHolders);
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
