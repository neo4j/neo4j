/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.query;

import java.util.Comparator;
import java.util.List;

import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.helpers.collection.Iterables.asList;

public abstract class QueryEngineProvider extends Service
{
    public QueryEngineProvider( String name )
    {
        super( name );
    }

    protected abstract QueryExecutionEngine createEngine( Dependencies deps, GraphDatabaseAPI graphAPI );

    protected abstract int enginePriority();

    public static QueryExecutionEngine initialize( Dependencies deps, GraphDatabaseAPI graphAPI,
            Iterable<QueryEngineProvider> providers )
    {
        List<QueryEngineProvider> engineProviders = asList( providers );
        engineProviders.sort( Comparator.comparingInt( QueryEngineProvider::enginePriority ) );
        QueryEngineProvider provider = Iterables.firstOrNull( engineProviders );

        if ( provider == null )
        {
            return noEngine();
        }
        QueryExecutionEngine engine = provider.createEngine( deps, graphAPI );
        return deps.satisfyDependency( engine );
    }

    public static QueryExecutionEngine noEngine()
    {
        return NoQueryEngine.INSTANCE;
    }
}
