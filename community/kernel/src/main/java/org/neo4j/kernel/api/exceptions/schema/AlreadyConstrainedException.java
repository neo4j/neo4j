/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;

public class AlreadyConstrainedException extends SchemaKernelException
{
    private final static String NO_CONTEXT_FORMAT = "Already constrained %s.";

    private static final String CONSTRAINT_CONTEXT_FORMAT = "Label '%s' and property '%s' already have a unique constraint defined on them.";
    private static final String INDEX_CONTEXT_FORMAT = "Label '%s' and property '%s' have a unique constraint defined on them, so an index is " +
                                                       "already created that matches this.";

    private final UniquenessConstraint constraint;
    private final OperationContext context;

    public AlreadyConstrainedException( UniquenessConstraint constraint, OperationContext context )
    {
        super( Status.Schema.ConstraintAlreadyExists, constructUserMessage( context, null, constraint ) );
        this.constraint = constraint;
        this.context = context;
    }

    private static String constructUserMessage( OperationContext context, TokenNameLookup tokenNameLookup, UniquenessConstraint constraint )
    {
        switch ( context )
        {
            case INDEX_CREATION:
                return messageWithLabelAndPropertyName( tokenNameLookup, INDEX_CONTEXT_FORMAT,
                        constraint.label(), constraint.propertyKeyId() );
            case CONSTRAINT_CREATION:
                return messageWithLabelAndPropertyName( tokenNameLookup, CONSTRAINT_CONTEXT_FORMAT,
                        constraint.label(), constraint.propertyKeyId() );
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
