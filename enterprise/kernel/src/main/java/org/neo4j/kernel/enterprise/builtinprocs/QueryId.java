/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

public final class QueryId
{
    public static final String QUERY_ID_PREFIX = "query-";
    private final long kernelQueryId;

    public static QueryId ofInternalId( long queryId ) throws InvalidArgumentsException
    {
        return new QueryId( queryId );
    }

    public static QueryId fromExternalString( String queryIdText ) throws InvalidArgumentsException
    {
        try
        {
            if ( queryIdText.startsWith( QUERY_ID_PREFIX ) )
            {
                return new QueryId( Long.parseLong( queryIdText.substring( QUERY_ID_PREFIX.length() ) ) );
            }
        }
        catch ( NumberFormatException e )
        {
            throw new InvalidArgumentsException( "Could not parse query id (expected format: query-1234)", e );
        }

        throw new InvalidArgumentsException( "Could not parse query id (expected format: query-1234)" );
    }

    private QueryId( long kernelQueryId ) throws InvalidArgumentsException
    {
        if ( kernelQueryId <= 0 )
        {
            throw new InvalidArgumentsException( "Negative query ids are not supported (expected format: query-1234)" );
        }
        this.kernelQueryId = kernelQueryId;
    }

    public long kernelQueryId()
    {
        return kernelQueryId;
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
        return kernelQueryId == other.kernelQueryId;
    }

    @Override
    public int hashCode()
    {
        return (int) (kernelQueryId ^ (kernelQueryId >>> 32));
    }

    @Override
    public String toString()
    {
        return QUERY_ID_PREFIX + kernelQueryId;
    }
}
