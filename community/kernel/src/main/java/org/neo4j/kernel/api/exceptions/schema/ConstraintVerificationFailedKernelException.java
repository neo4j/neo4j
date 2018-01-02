/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Constraint verification happens when a new constraint is created, and the database verifies that existing
 * data adheres to the new constraint.
 *
 * @see ConstraintViolationKernelException
 */
public abstract class ConstraintVerificationFailedKernelException extends KernelException
{
    protected ConstraintVerificationFailedKernelException( PropertyConstraint constraint )
    {
        super( Status.Schema.ConstraintVerificationFailure, "Existing data does not satisfy %s.", constraint );
    }

    protected ConstraintVerificationFailedKernelException( PropertyConstraint constraint, Throwable failure )
    {
        super( Status.Schema.ConstraintVerificationFailure, failure, "Failed to verify constraint %s: %s", constraint,
                failure.getMessage() );
    }

    @Override
    public abstract String getUserMessage( TokenNameLookup tokenNameLookup );

    public abstract PropertyConstraint constraint();
}
