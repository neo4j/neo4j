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

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.constraints.RelationshipEndpointConstraintDescriptor;

public final class RelationshipEndpointMissingLabelException extends ConstraintValidationException {
    private final long relationshipReference;
    private final Phase phase;
    private final RelationshipEndpointConstraintDescriptor descriptor;

    public RelationshipEndpointMissingLabelException(
            RelationshipEndpointConstraintDescriptor descriptor,
            Phase phase,
            long relationshipReference,
            TokenNameLookup tokenNameLookup) {
        super(descriptor, phase, "Relationship(" + relationshipReference + ")", tokenNameLookup);
        this.relationshipReference = relationshipReference;
        this.phase = phase;
        this.descriptor = descriptor;
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        return format(
                "Relationship(%s) with type %s requires it's %s Node(%s) to have label %s",
                relationshipReference,
                tokenNameLookup.relationshipTypeGetName(descriptor.schema().getRelTypeId()),
                descriptor.endpointType().name(),
                relationshipReference,
                tokenNameLookup.labelGetName(descriptor.endpointLabelId()));
    }
}
