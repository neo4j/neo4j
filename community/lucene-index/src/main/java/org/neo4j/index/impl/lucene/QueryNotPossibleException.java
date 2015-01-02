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
package org.neo4j.index.impl.lucene;

/**
 * Querying an index inside a transaction where
 * modifications has been made must be explicitly enabled using
 * QueryContext.allowQueryingModifications
 */
public class QueryNotPossibleException extends RuntimeException
{
    public QueryNotPossibleException()
    {
        super();
    }

    public QueryNotPossibleException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public QueryNotPossibleException( String message )
    {
        super( message );
    }

    public QueryNotPossibleException( Throwable cause )
    {
        super( cause );
    }
}
