/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api;

import java.util.Map;

import static java.lang.String.format;

/**
 * Represents a currently running query.
 */
public class ExecutingQuery
{
    private final long queryId;

    private final String authSubjectName;
    private final String queryText;
    private final Map<String, Object> queryParameters;
    private final long startTime;

    public ExecutingQuery(
        long queryId,
        String authSubjectName,
        String queryText,
        Map<String,Object> queryParameters,
        long startTime
    )
    {
        this.queryId = queryId;
        this.authSubjectName = authSubjectName;
        this.queryText = queryText;
        this.queryParameters = queryParameters;
        this.startTime = startTime;
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

        ExecutingQuery that = (ExecutingQuery) o;

        return queryId == that.queryId;

    }

    @Override
    public int hashCode()
    {
        return (int) (queryId ^ (queryId >>> 32));
    }

    public long kernelQueryId()
    {
        return queryId;
    }

    public String authSubjectName()
    {
        return authSubjectName;
    }

    public String queryText()
    {
        return queryText;
    }

    public Map<String,Object> queryParameters()
    {
        return queryParameters;
    }

    public long startTime()
    {
        return startTime;
    }

    @Override
    public String toString()
    {
        return format(
            "ExecutingQuery{queryId=%d, authSubjectName='%s', queryText='%s', queryParameters=%s, startTime=%d}",
            queryId, authSubjectName, queryText, queryParameters, startTime );
    }
}
