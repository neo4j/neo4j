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

import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.Status;

public class CreateConstraintFailureException extends SchemaKernelException
{
    private final PropertyConstraint constraint;

    public CreateConstraintFailureException( PropertyConstraint constraint, Throwable cause )
    {
        super( Status.Schema.ConstraintCreationFailure, cause, "Unable to create constraint %s: %s", constraint,
                cause.getMessage() );
        this.constraint = constraint;
    }

    public PropertyConstraint constraint()
    {
        return constraint;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        String message = "Unable to create " + constraint.userDescription( tokenNameLookup );
        if ( getCause() instanceof KernelException )
        {
            KernelException cause = (KernelException) getCause();

            return String.format( "%s:%n%s", message, cause.getUserMessage( tokenNameLookup ) );
        }
        return message;
    }
}
