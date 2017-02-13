/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.index.IndexUsage;

import static java.util.Collections.emptyList;

public class QueryInfo
{
    public final String text;
    public final Map<String,Object> parameters;
    public final String planner;
    public final String runtime;
    private final List<IndexUsage> indexes;

    public QueryInfo( String text, Map<String,Object> parameters, PlannerInfo plannerInfo )
    {
        this.text = text;
        this.parameters = parameters;
        if ( plannerInfo != null )
        {
            this.planner = plannerInfo.planner();
            this.runtime = plannerInfo.runtime();
            this.indexes = plannerInfo.indexes();
        }
        else
        {
            this.planner = null;
            this.runtime = null;
            this.indexes = emptyList();
        }
    }

    public List<Map<String,String>> indexes()
    {
        List<Map<String,String>> used = new ArrayList<>( this.indexes.size() );
        for ( IndexUsage index : indexes )
        {
            used.add( index.asMap() );
        }
        return used;
    }
}
