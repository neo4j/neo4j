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
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

public class IncompatibleGraphTypeDependenceException extends SchemaKernelException {

    public IncompatibleGraphTypeDependenceException(
            ConstraintDescriptor constraint,
            ConstraintDescriptor preExistingConstraint,
            TokenNameLookup tokenNameLookup) {
        super(
                Status.Schema.ConstraintCreationFailed,
                constructUserMessage(constraint, preExistingConstraint, tokenNameLookup));
    }

    private static String constructUserMessage(
            ConstraintDescriptor constraint,
            ConstraintDescriptor preExistingConstraint,
            TokenNameLookup tokenNameLookup) {
        return format(
                "Graph Type %s constraint: %s is incompatible with Graph Type %s %s due to differing Graph Type dependence.",
                constraint.graphTypeDependence().name().toLowerCase(Locale.ROOT),
                constraint.schema().userDescription(tokenNameLookup),
                preExistingConstraint.graphTypeDependence().name().toLowerCase(Locale.ROOT),
                preExistingConstraint.userDescription(tokenNameLookup));
    }
}
