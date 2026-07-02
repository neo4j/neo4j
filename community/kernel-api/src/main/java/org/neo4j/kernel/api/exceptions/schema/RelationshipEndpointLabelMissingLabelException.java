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

import java.util.Locale;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.constraints.RelationshipEndpointLabelConstraintDescriptor;

public final class RelationshipEndpointLabelMissingLabelException extends ConstraintValidationException {
    private final long relationshipReference;
    private final RelationshipEndpointLabelConstraintDescriptor descriptor;
    private final long nodeReference;

    private RelationshipEndpointLabelMissingLabelException(
            ErrorGqlStatusObject gqlStatusObject,
            RelationshipEndpointLabelConstraintDescriptor descriptor,
            Phase phase,
            long relationshipReference,
            long nodeReference,
            TokenNameLookup tokenNameLookup) {
        super(gqlStatusObject, descriptor, phase, "Relationship(" + relationshipReference + ")", tokenNameLookup);
        this.relationshipReference = relationshipReference;
        this.descriptor = descriptor;
        this.nodeReference = nodeReference;
    }

    // KNL-194
    public static RelationshipEndpointLabelMissingLabelException endpointLabelPresenceVerificationFailed(
            RelationshipEndpointLabelConstraintDescriptor descriptor,
            Phase phase,
            long relationshipReference,
            long nodeReference,
            TokenNameLookup tokenNameLookup) {
        ErrorGqlStatusObject gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB4)
                .withParam(GqlParams.NumberParam.entityId1, relationshipReference)
                .withParam(
                        GqlParams.StringParam.relType,
                        tokenNameLookup.relationshipTypeGetName(
                                descriptor.schema().getRelTypeId()))
                .withParam(
                        GqlParams.StringParam.endpointType,
                        descriptor.endpointType().name().toLowerCase(Locale.ROOT))
                .withParam(GqlParams.NumberParam.entityId2, nodeReference)
                .withParam(GqlParams.StringParam.label, tokenNameLookup.labelGetName(descriptor.endpointLabelId()))
                .build();
        return new RelationshipEndpointLabelMissingLabelException(
                gql, descriptor, phase, relationshipReference, nodeReference, tokenNameLookup);
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        return format(
                "Relationship(%s) with type %s requires its %s Node(%s) to have label %s",
                relationshipReference,
                tokenNameLookup.relationshipTypeGetName(descriptor.schema().getRelTypeId()),
                descriptor.endpointType().name().toLowerCase(Locale.ROOT),
                nodeReference,
                tokenNameLookup.labelGetName(descriptor.endpointLabelId()));
    }
}
