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
package org.neo4j.kernel.impl.proc;

import java.net.URL;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;

public class ProcedureGDSFactory implements ThrowingFunction<Context,GraphDatabaseService,ProcedureException>
{
    private final PlatformModule platform;
    private final EditionModule editionModule;
    private final DataSourceModule dataSource;
    private final DependencyResolver resolver;
    private final CoreAPIAvailabilityGuard availability;
    private final ThrowingFunction<URL, URL, URLAccessValidationError> urlValidator;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;

    public ProcedureGDSFactory( PlatformModule platform, EditionModule editionModule, DataSourceModule dataSource, DependencyResolver resolver,
            CoreAPIAvailabilityGuard coreAPIAvailabilityGuard, RelationshipTypeTokenHolder relationshipTypeTokenHolder )
    {
        this.platform = platform;
        this.editionModule = editionModule;
        this.dataSource = dataSource;
        this.resolver = resolver;
        this.availability = coreAPIAvailabilityGuard;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.urlValidator = url -> platform.urlAccessRule.validate( platform.config, url );
    }

    @Override
    public GraphDatabaseService apply( Context context ) throws ProcedureException
    {
        KernelTransaction tx = context.getOrElse( Context.KERNEL_TRANSACTION, null );
        SecurityContext securityContext;
        if ( tx != null )
        {
            securityContext = tx.securityContext();
        }
        else
        {
            securityContext = context.get( Context.SECURITY_CONTEXT );
        }
        GraphDatabaseFacade facade = new GraphDatabaseFacade();
        facade.init(
            editionModule,
            new ProcedureGDBFacadeSPI(
                platform,
                dataSource,
                resolver,
                availability,
                urlValidator,
                securityContext
            ),
            dataSource.guard,
            dataSource.threadToTransactionBridge,
            platform.config,
            relationshipTypeTokenHolder
        );
        return facade;
    }
}
