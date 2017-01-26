/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.cypher.internal.CommunityCompatibilityFactory;
import org.neo4j.cypher.internal.EnterpriseCompatibilityFactory;
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

@Service.Implementation(QueryEngineProvider.class)
public class EnterpriseCypherEngineProvider extends QueryEngineProvider
{
    public EnterpriseCypherEngineProvider()
    {
        super( "enterprise-cypher" );
    }

    @Override
    protected int enginePriority()
    {
        return 1; // Lower means better. The enterprise version will have a lower number
    }

    @Override
    protected QueryExecutionEngine createEngine( Dependencies deps, GraphDatabaseAPI graphAPI )
    {
        GraphDatabaseCypherService queryService = new GraphDatabaseCypherService( graphAPI );
        deps.satisfyDependency( queryService );

        DependencyResolver resolver = graphAPI.getDependencyResolver();
        LogService logService = resolver.resolveDependency( LogService.class );
        KernelAPI kernelAPI = resolver.resolveDependency( KernelAPI.class );
        Monitors monitors = resolver.resolveDependency( Monitors.class );
        LogProvider logProvider = logService.getInternalLogProvider();
        CommunityCompatibilityFactory inner =
                new CommunityCompatibilityFactory( queryService, kernelAPI, monitors, logProvider );

        EnterpriseCompatibilityFactory compatibilityFactory =
                new EnterpriseCompatibilityFactory( inner, queryService, kernelAPI, monitors, logProvider );
        deps.satisfyDependency( compatibilityFactory );
        return new ExecutionEngine( queryService, logProvider, compatibilityFactory );
    }
}
