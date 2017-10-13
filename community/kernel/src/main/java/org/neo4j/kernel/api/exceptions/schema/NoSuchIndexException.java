/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaUtil;

import static java.lang.String.format;

public class NoSuchIndexException extends SchemaKernelException
{
    private final LabelSchemaDescriptor descriptor;
    private static final String message = "No such INDEX ON %s.";

    public NoSuchIndexException( LabelSchemaDescriptor descriptor )
    {
        super( Status.Schema.IndexNotFound, format( message,
                descriptor.userDescription( SchemaUtil.idTokenNameLookup ) ) );
        this.descriptor = descriptor;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return format( message, descriptor.userDescription( tokenNameLookup ) );
    }
}
