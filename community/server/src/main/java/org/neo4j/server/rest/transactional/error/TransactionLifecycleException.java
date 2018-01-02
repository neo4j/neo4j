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
package org.neo4j.server.rest.transactional.error;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * TransactionLifecycleExceptions are internal exceptions that may be thrown
 * due to server transaction lifecycle transitions that map directly on a
 * {@link Status.Code}.
 */
public abstract class TransactionLifecycleException extends Exception
{
    protected TransactionLifecycleException( String message )
    {
        super( message );
    }

    protected TransactionLifecycleException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public Neo4jError toNeo4jError()
    {
        return new Neo4jError( getStatusCode(), this );
    }

    protected abstract Status getStatusCode();
}
