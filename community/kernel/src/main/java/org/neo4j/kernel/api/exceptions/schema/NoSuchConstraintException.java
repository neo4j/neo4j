/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;

public class NoSuchConstraintException extends SchemaKernelException
{
    private final SchemaDescriptorSupplier constraint;
    private final String name;
    private static final String MESSAGE = "No such constraint %s.";

    public NoSuchConstraintException( SchemaDescriptorSupplier constraint, TokenNameLookup lookup )
    {
        super( Status.Schema.ConstraintNotFound, format( MESSAGE, constraint.userDescription( lookup ) ) );
        this.constraint = constraint;
        this.name = "";
    }

    public NoSuchConstraintException( String name )
    {
        super( Status.Schema.ConstraintNotFound, format( MESSAGE, name ) );
        this.constraint = null;
        this.name = name;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        if ( constraint == null )
        {
            return format( MESSAGE, name );
        }
        else
        {
            return format( MESSAGE, constraint.userDescription( tokenNameLookup ) );
        }
    }
}
