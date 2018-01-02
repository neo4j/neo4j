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
package org.neo4j.kernel.impl.query;

import java.util.Map;

import org.neo4j.graphdb.Result;

enum NoQueryEngine implements QueryExecutionEngine
{
    INSTANCE;

    @Override
    public Result executeQuery( String query, Map<String, Object> parameters, QuerySession querySession )
    {
        throw noQueryEngine();
    }

    @Override
    public String prettify( String query )
    {
        throw noQueryEngine();
    }

    @Override
    public Result profileQuery( String query, Map<String, Object> parameter, QuerySession session )
    {
        throw noQueryEngine();
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        throw noQueryEngine();
    }

    private RuntimeException noQueryEngine()
    {
        return new UnsupportedOperationException( "No query engine installed." );
    }
}
