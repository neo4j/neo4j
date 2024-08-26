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
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.LabelCoexistenceConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.LabelCoexistenceMissingLabelException;

public final class LabelCoexistenceMissingLabelExceptionTest {
    private static final int SCHEMA_LABEL_ID = 0;
    private static final int REQUIRED_LABEL_ID = 1;

    @Test
    public void shouldGetCorrectUserMessage() {
        var kernelToken = mock(TokenNameLookup.class);

        when(kernelToken.labelGetName(SCHEMA_LABEL_ID)).thenReturn("SchemaLabel");
        when(kernelToken.labelGetName(REQUIRED_LABEL_ID)).thenReturn("RequiredLabel");

        LabelCoexistenceConstraintDescriptor constraintDescriptor =
                ConstraintDescriptorFactory.labelCoexistenceForLabel(SCHEMA_LABEL_ID, REQUIRED_LABEL_ID);
        var userMessage = new LabelCoexistenceMissingLabelException(
                        constraintDescriptor,
                        ConstraintValidationException.Phase.VERIFICATION,
                        SCHEMA_LABEL_ID,
                        kernelToken)
                .getUserMessage(kernelToken);

        assertThat(userMessage).isEqualTo("Node(0) with label SchemaLabel is required to have label RequiredLabel");
    }
}
