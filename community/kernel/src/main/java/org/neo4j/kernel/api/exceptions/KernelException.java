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
package org.neo4j.kernel.api.exceptions;

import org.neo4j.kernel.api.TokenNameLookup;

/** A super class of checked exceptions coming from the {@link org.neo4j.kernel.api.KernelAPI Kernel API}. */
public abstract class KernelException extends Exception implements Status.HasStatus
{
    private final Status statusCode;

    protected KernelException( Status statusCode, Throwable cause, String message, Object... parameters )
    {
        super( (parameters.length > 0) ? String.format( message, parameters ) : message );
        this.statusCode = statusCode;
        initCause( cause );
    }

    protected KernelException( Status statusCode, String message, Object... parameters )
    {
        super( String.format( message, parameters ) );
        this.statusCode = statusCode;
    }

    /** The Neo4j status code associated with this exception type. */
    @Override
    public Status status()
    {
        return statusCode;
    }

    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return getMessage();
    }
}
