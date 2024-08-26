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
import org.neo4j.internal.schema.constraints.LabelCoexistenceConstraintDescriptor;

public final class LabelCoexistenceMissingLabelException extends ConstraintValidationException {
    private final LabelCoexistenceConstraintDescriptor descriptor;
    private final long nodeReference;

    public LabelCoexistenceMissingLabelException(
            LabelCoexistenceConstraintDescriptor descriptor,
            Phase phase,
            long nodeReference,
            TokenNameLookup tokenNameLookup) {
        super(descriptor, phase, format("Node(%d)", nodeReference), tokenNameLookup);
        this.descriptor = descriptor;
        this.nodeReference = nodeReference;
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
