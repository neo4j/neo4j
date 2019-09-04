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

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;
import static org.neo4j.common.TokenNameLookup.idTokenNameLookup;

public class NoSuchIndexException extends SchemaKernelException
{
    private final SchemaDescriptor descriptor;
    private final String name;
    private static final String message = "No such INDEX %s.";

    public NoSuchIndexException( SchemaDescriptor descriptor )
    {
        super( Status.Schema.IndexNotFound, format( message, "ON " + descriptor.userDescription( idTokenNameLookup ) ) );
        this.descriptor = descriptor;
        this.name = "";
    }

    public NoSuchIndexException( String name )
    {
        super( Status.Schema.IndexNotFound, format( message, name ) );
        this.descriptor = null;
        this.name = name;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        if ( descriptor == null )
        {
            return format( message, name );
        }
        else
        {
            return format( message, "ON " + descriptor.userDescription( tokenNameLookup ) );
        }
    }
}
