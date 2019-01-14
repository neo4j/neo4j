/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api.exceptions.schema;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Constraint verification happens when a new constraint is created, and the database verifies that existing
 * data adheres to the new constraint.
 */
public abstract class ConstraintValidationException extends KernelException
{
    /**
     * Constraint validation failures can happen during one of two phases of the constraint lifecycle.
     *
     * VERIFICATION is the process to control that a constraint holds with respect to all the data in the graph. This
     * happens before creating a constraint for example, and if the verification fails the constraint is not created.
     * Verification can also occur during batch import for example.
     *
     * VALIDATION is what happens when the graph is modified, and the resulting state is controlled against a
     * constraint to see that the modified state does not violate the constraint. If validation fails the modifying
     * transaction is rolled back.
     */
    public enum Phase
    {
        VERIFICATION( Status.Statement.ConstraintVerificationFailed ),
        VALIDATION( Status.Schema.ConstraintValidationFailed );

        private final Status status;

        Phase( Status status )
        {
            this.status = status;
        }

        public Status getStatus()
        {
            return status;
        }
    }

    protected final ConstraintDescriptor constraint;

    protected ConstraintValidationException( ConstraintDescriptor constraint, Phase phase, String subject )
    {
        super( phase.getStatus(), "%s does not satisfy %s.",
                subject, constraint.prettyPrint( SchemaUtil.idTokenNameLookup ) );
        this.constraint = constraint;
    }

    protected ConstraintValidationException( ConstraintDescriptor constraint, Phase phase, String subject, Throwable failure )
    {
        super( phase.getStatus(), failure, "%s does not satisfy %s: %s",
                subject, constraint.prettyPrint( SchemaUtil.idTokenNameLookup ), failure.getMessage() );
        this.constraint = constraint;
    }

    @Override
    public abstract String getUserMessage( TokenNameLookup tokenNameLookup );

    public ConstraintDescriptor constraint()
    {
        return constraint;
    }
}
