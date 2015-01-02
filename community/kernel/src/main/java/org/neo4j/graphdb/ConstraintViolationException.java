/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
 * Thrown when the database is asked to modify data in a way that violates one or more
 * constraints that it is expected to uphold.
 *
 * For instance, if removing a node that still has relationships.
 */
public class ConstraintViolationException extends RuntimeException
{
    public ConstraintViolationException( String msg )
    {
        super(msg);
    }

    public ConstraintViolationException( String msg, Throwable cause )
    {
        super(msg, cause);
    }
}
