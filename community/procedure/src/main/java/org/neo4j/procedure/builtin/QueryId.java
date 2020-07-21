/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.procedure.builtin;

import java.util.Objects;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

public class QueryId
{
    public static final String PREFIX = "query-";
    private static final String EXPECTED_FORMAT_MSG = "(expected format: query-<id>)";
    private final long internalId;

    public QueryId( long internalId ) throws InvalidArgumentsException
    {
        if ( internalId <= 0 )
        {
            throw new InvalidArgumentsException( "Negative ids are not supported " + EXPECTED_FORMAT_MSG );
        }
        this.internalId = internalId;
    }

    public static QueryId parse( String queryIdText ) throws InvalidArgumentsException
    {
        try
        {
            if ( !queryIdText.startsWith( PREFIX ) )
            {
                throw new InvalidArgumentsException( "Expected prefix " + PREFIX );
            }
            String qid = queryIdText.substring( PREFIX.length() );
            var internalId = Long.parseLong( qid );
            return new QueryId( internalId );
        }
        catch ( Exception e )
        {
            throw new InvalidArgumentsException( "Could not parse id " + queryIdText + " " + EXPECTED_FORMAT_MSG, e );
        }
    }

    public long internalId()
    {
        return internalId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        QueryId other = (QueryId) o;
        return internalId == other.internalId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( internalId );
    }

    @Override
    public String toString()
    {
        return PREFIX + internalId;
    }
}
