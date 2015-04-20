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

import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.index.IndexDescriptor;

import static java.lang.String.format;

public class AddIndexFailureException extends SchemaKernelException
{
    private final static String MESSAGE = "Unable to add index %s : %s";

    private final int labelId;
    private final int propertyKey;

    public AddIndexFailureException( int labelId, int propertyKey, KernelException cause )
    {
        super( Status.Schema.IndexCreationFailure, format( MESSAGE, new IndexDescriptor( labelId, propertyKey ), cause.getMessage() ), cause );
        this.labelId = labelId;
        this.propertyKey = propertyKey;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return String.format( format( MESSAGE,
                new IndexDescriptor( labelId, propertyKey ).userDescription( tokenNameLookup ),
                ((KernelException) getCause()).getUserMessage( tokenNameLookup ) ) );
    }
}
