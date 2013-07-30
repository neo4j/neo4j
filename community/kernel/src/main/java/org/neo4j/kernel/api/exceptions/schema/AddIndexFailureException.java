/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.api.operations.TokenNameLookup;

import static java.lang.String.format;

public class AddIndexFailureException extends SchemaKernelException
{
    private final static String message = "Unable to add index on [label: %s, %s] : %s";

    private final long labelId;
    private final long propertyKey;

    public AddIndexFailureException( long labelId, long propertyKey, KernelException cause )
    {
        super( format( message, labelId, propertyKey, cause.getMessage() ), cause );
        this.labelId = labelId;
        this.propertyKey = propertyKey;
    }

    @Override
    public String getUserMessage( TokenNameLookup nameLookup )
    {
        return format( message, nameLookup.labelGetName( labelId ), nameLookup.propertyKeyGetName( propertyKey ),
                ((KernelException) getCause()).getUserMessage( nameLookup ) );
    }
}
