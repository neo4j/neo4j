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

/**
 * This exception is thrown when committing an updating transaction if
 * {@link org.neo4j.graphdb.factory.GraphDatabaseSettings} read_only has been set to true. Can also be thrown when
 * trying to create tokens (like new property names) in a read only database.
 */

public class ReadOnlyDbException extends TransactionFailureException
{
    public ReadOnlyDbException()
    {
        super( Status.General.ReadOnly, "This is a read only Neo4j instance" );
    }
}