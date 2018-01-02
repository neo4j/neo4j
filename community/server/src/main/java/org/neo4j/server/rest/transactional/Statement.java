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
package org.neo4j.server.rest.transactional;

import java.util.Map;

public class Statement
{
    private final String statement;
    private final Map<String, Object> parameters;
    private final boolean includeStats;
    private final ResultDataContent[] resultDataContents;

    public Statement( String statement, Map<String, Object> parameters, boolean includeStats,
                      ResultDataContent... resultDataContents )
    {
        this.statement = statement;
        this.parameters = parameters;
        this.includeStats = includeStats;
        this.resultDataContents = resultDataContents;
    }

    public String statement()
    {
        return statement;
    }

    public Map<String, Object> parameters()
    {
        return parameters;
    }

    public ResultDataContent[] resultDataContents()
    {
        return resultDataContents;
    }

    public boolean includeStats()
    {
        return includeStats;
    }
}
