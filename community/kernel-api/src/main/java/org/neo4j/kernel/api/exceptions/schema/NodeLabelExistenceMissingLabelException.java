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

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.constraints.NodeLabelExistenceConstraintDescriptor;
import org.neo4j.token.api.TokenType;

public final class NodeLabelExistenceMissingLabelException extends ConstraintValidationException {
    private final NodeLabelExistenceConstraintDescriptor descriptor;
    private final long nodeReference;

    private NodeLabelExistenceMissingLabelException(
            ErrorGqlStatusObject gqlStatusObject,
            NodeLabelExistenceConstraintDescriptor descriptor,
            Phase phase,
            long nodeReference,
            TokenNameLookup tokenNameLookup) {
        super(gqlStatusObject, descriptor, phase, format("Node(%d)", nodeReference), tokenNameLookup);
        this.descriptor = descriptor;
        this.nodeReference = nodeReference;
    }

    // KNL-193
    public static NodeLabelExistenceMissingLabelException tokenPresenceVerificationFailed(
            NodeLabelExistenceConstraintDescriptor descriptor,
            Phase phase,
            long nodeReference,
            TokenNameLookup tokenNameLookup) {
        ErrorGqlStatusObject gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB3)
                .withParam(GqlParams.StringParam.entityType, EntityType.NODE.name())
                .withParam(GqlParams.NumberParam.entityId, nodeReference)
                .withParam(GqlParams.StringParam.tokenType1, TokenType.LABEL.getName())
                .withParam(GqlParams.StringParam.token1, tokenNameLookup.labelGetName(descriptor.schemaLabelId()))
                .withParam(GqlParams.StringParam.tokenType2, TokenType.LABEL.getName())
                .withParam(GqlParams.StringParam.token2, tokenNameLookup.labelGetName(descriptor.requiredLabelId()))
                .build();
        return new NodeLabelExistenceMissingLabelException(gql, descriptor, phase, nodeReference, tokenNameLookup);
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        return format(
                "Node(%d) with label %s is required to have label %s",
                nodeReference,
                tokenNameLookup.labelGetName(descriptor.schema().getLabelId()),
                tokenNameLookup.labelGetName(descriptor.requiredLabelId()));
    }
}
