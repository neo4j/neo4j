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
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

public class AlreadyIndexedException extends SchemaKernelException
{
    private static final String NO_CONTEXT_FORMAT = "Already indexed %s.";

    private static final String INDEX_CONTEXT_FORMAT = "There already exists an index for label '%s' on %s.";
    private static final String CONSTRAINT_CONTEXT_FORMAT = "There already exists an index for label '%s' on %s. " +
                                                            "A constraint cannot be created until the index has been dropped.";

    private final SchemaDescriptor descriptor;
    private final OperationContext context;

    public AlreadyIndexedException( SchemaDescriptor descriptor, OperationContext context )
    {
        super( Status.Schema.IndexAlreadyExists, constructUserMessage( context, null, descriptor ) );

        this.descriptor = descriptor;
        this.context = context;
    }

    private static String constructUserMessage( OperationContext context, TokenNameLookup tokenNameLookup,
            SchemaDescriptor descriptor )
    {
        switch ( context )
        {
            case INDEX_CREATION:
                return messageWithLabelAndPropertyName( tokenNameLookup, INDEX_CONTEXT_FORMAT, descriptor );
            case CONSTRAINT_CREATION:
                return messageWithLabelAndPropertyName( tokenNameLookup, CONSTRAINT_CONTEXT_FORMAT, descriptor );
            default:
                return String.format( NO_CONTEXT_FORMAT, descriptor );
        }
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return constructUserMessage( context, tokenNameLookup, descriptor );
    }
}
