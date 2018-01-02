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
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Signals that some constraint has been violated in a {@link org.neo4j.kernel.api.KernelAPI kernel interaction},
 * for example a name containing invalid characters or length.
 */
public abstract class SchemaKernelException extends KernelException
{
    public enum OperationContext
    {
        INDEX_CREATION,
        CONSTRAINT_CREATION
    }

    protected SchemaKernelException( Status statusCode, Throwable cause, String message, Object... parameters )
    {
        super( statusCode, cause, message, parameters );
    }

    public SchemaKernelException( Status statusCode, String message, Throwable cause )
    {
        super( statusCode, cause, message );
    }

    public SchemaKernelException( Status statusCode, String message )
    {
        super( statusCode, message );
    }

    protected static String messageWithLabelAndPropertyName( TokenNameLookup tokenNameLookup, String formatString,
            int labelId, int propertyKeyId )
    {
        if ( tokenNameLookup != null )
        {
            return String.format( formatString,
                    tokenNameLookup.labelGetName( labelId ),
                    tokenNameLookup.propertyKeyGetName( propertyKeyId ) );
        }
        else
        {
            return String.format( formatString,
                    "label[" + labelId + "]",
                    "key[" + propertyKeyId + "]" );
        }
    }
}
