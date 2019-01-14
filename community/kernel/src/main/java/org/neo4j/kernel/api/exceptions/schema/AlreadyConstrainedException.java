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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;

public class AlreadyConstrainedException extends SchemaKernelException
{
    private static final String NO_CONTEXT_FORMAT = "Already constrained %s.";

    private static final String ALREADY_CONSTRAINED_MESSAGE_PREFIX = "Constraint already exists: ";

    private static final String INDEX_CONTEXT_FORMAT = "Label '%s' and %s have a unique constraint defined on them, so an index is " +
                                                       "already created that matches this.";

    private final ConstraintDescriptor constraint;
    private final OperationContext context;

    public AlreadyConstrainedException( ConstraintDescriptor constraint, OperationContext context,
            TokenNameLookup tokenNameLookup )
    {
        super( Status.Schema.ConstraintAlreadyExists, constructUserMessage( context, tokenNameLookup, constraint ) );
        this.constraint = constraint;
        this.context = context;
    }

    public ConstraintDescriptor constraint()
    {
        return constraint;
    }

    private static String constructUserMessage( OperationContext context, TokenNameLookup tokenNameLookup,
            ConstraintDescriptor constraint )
    {
        switch ( context )
        {
            case INDEX_CREATION:
                return messageWithLabelAndPropertyName( tokenNameLookup, INDEX_CONTEXT_FORMAT, constraint.schema() );

            case CONSTRAINT_CREATION:
                return ALREADY_CONSTRAINED_MESSAGE_PREFIX + constraint.prettyPrint( tokenNameLookup );

            default:
                return format( NO_CONTEXT_FORMAT, constraint );
        }
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return constructUserMessage( context, tokenNameLookup, constraint );
    }
}
