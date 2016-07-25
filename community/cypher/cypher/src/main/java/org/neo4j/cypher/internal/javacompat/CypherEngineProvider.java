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
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;

@Service.Implementation(QueryEngineProvider.class)
public class CypherEngineProvider extends QueryEngineProvider
{
    public CypherEngineProvider()
    {
        super( "cypher" );
    }

    @Override
    protected QueryExecutionEngine createEngine( GraphDatabaseAPI graphAPI )
    {
        LogService logService = graphAPI.getDependencyResolver().resolveDependency( LogService.class );
        return new ExecutionEngine( new GraphDatabaseCypherService( graphAPI ), logService.getInternalLogProvider() );
    }
}
