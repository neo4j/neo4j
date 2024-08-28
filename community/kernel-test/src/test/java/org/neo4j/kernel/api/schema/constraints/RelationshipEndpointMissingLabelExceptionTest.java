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
package org.neo4j.kernel.api.schema.constraints;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.EndpointType;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.RelationshipEndpointConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.RelationshipEndpointMissingLabelException;

public final class RelationshipEndpointMissingLabelExceptionTest {
    private static final int LABEL_ID = 0;
    private static final int REL_TYPE_ID = 0;

    @Test
    public void shouldGetCorrectUserMessage() {
        var kernelToken = mock(TokenNameLookup.class);

        when(kernelToken.relationshipTypeGetName(REL_TYPE_ID)).thenReturn("RelationshipType");
        when(kernelToken.labelGetName(LABEL_ID)).thenReturn("EndpointLabel");
        long relationshipReference = 1;
        long nodeReference = 2;
        RelationshipEndpointConstraintDescriptor endpointConstraintDescriptor =
                ConstraintDescriptorFactory.relationshipEndpointForRelType(REL_TYPE_ID, LABEL_ID, EndpointType.START);
        var userMessage = new RelationshipEndpointMissingLabelException(
                        endpointConstraintDescriptor,
                        ConstraintValidationException.Phase.VERIFICATION,
                        relationshipReference,
                        nodeReference,
                        kernelToken)
                .getUserMessage(kernelToken);

        assertThat(userMessage)
                .isEqualTo(
                        "Relationship(1) with type RelationshipType requires it's start Node(2) to have label EndpointLabel");
    }
}
