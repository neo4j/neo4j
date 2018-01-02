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
package org.neo4j.graphdb;

/**
 * Indicates a type of failure that is intermediate and, in a way benign.
 *
 * A proper response to a caught exception of this type is to cancel the unit of work that produced
 * this exception and retry the unit of work again, as a whole.
 */
public abstract class TransientFailureException extends RuntimeException
{
    protected TransientFailureException( String message, Throwable cause )
    {
        super( message, cause );
    }

    protected TransientFailureException( String message )
    {
        super( message );
    }
}
