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
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.token.api.TokenIdPrettyPrinter;

public class NodePropertyExistenceException extends ConstraintValidationException {
    private final long nodeId;
    private final LabelSchemaDescriptor schema;

    private NodePropertyExistenceException(
            ErrorGqlStatusObject gqlStatusObject,
            LabelSchemaDescriptor schema,
            Function<LabelSchemaDescriptor, ConstraintDescriptor> constraintFunc,
            ConstraintValidationException.Phase phase,
            long nodeId,
            TokenNameLookup tokenNameLookup) {
        super(gqlStatusObject, constraintFunc.apply(schema), phase, format("Node(%d)", nodeId), tokenNameLookup);

        this.schema = schema;
        this.nodeId = nodeId;
    }

    public static NodePropertyExistenceException propertyPresenceViolation(
            LabelSchemaDescriptor schema,
            TokenNameLookup tokenHolders,
            ConstraintDescriptor descriptor,
            ConstraintValidationException.Phase phase,
            long nodeId) {
        int[] propIds = schema.getPropertyIds();
        // This might be a way to expose hidden properties to the user with roles with no access to those properties
        // TODO: check for user rights
        String[] propKeyNames = new String[propIds.length];
        for (int i = 0; i < propIds.length; i++) {
            propKeyNames[i] = tokenHolders.propertyKeyGetName(propIds[i]);
        }
        ErrorGqlStatusObject gql =
                GqlHelper.getGql22N77_nodes(nodeId, tokenHolders.labelGetName(schema.getLabelId()), propKeyNames);
        return new NodePropertyExistenceException(gql, schema, ignored -> descriptor, phase, nodeId, tokenHolders);
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
                "Node(%d) with label `%s` must have the %s %s%s%s",
                nodeId, tokenNameLookup.labelGetName(schema.getLabelId()), propertyNoun, sep, props, sep);
    }
}
