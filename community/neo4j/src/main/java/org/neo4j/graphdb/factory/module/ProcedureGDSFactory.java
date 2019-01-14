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
package org.neo4j.graphdb.factory.module;

import java.net.URL;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.facade.spi.ProcedureGDBFacadeSPI;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class ProcedureGDSFactory implements ThrowingFunction<Context,GraphDatabaseService,ProcedureException>
{
    private final PlatformModule platform;
    private final DataSourceModule dataSource;
    private final CoreAPIAvailabilityGuard availability;
    private final ThrowingFunction<URL, URL, URLAccessValidationError> urlValidator;
    private final TokenHolders tokenHolders;
    private final ThreadToStatementContextBridge bridge;

    ProcedureGDSFactory( PlatformModule platform, DataSourceModule dataSource, CoreAPIAvailabilityGuard coreAPIAvailabilityGuard, TokenHolders tokenHolders,
            ThreadToStatementContextBridge bridge )
    {
        this.platform = platform;
        this.dataSource = dataSource;
        this.availability = coreAPIAvailabilityGuard;
        this.urlValidator = url -> platform.urlAccessRule.validate( platform.config, url );
        this.tokenHolders = tokenHolders;
        this.bridge = bridge;
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
        ProcedureGDBFacadeSPI procedureGDBFacadeSPI = new ProcedureGDBFacadeSPI( dataSource, dataSource.neoStoreDataSource.getDependencyResolver(),
                availability, urlValidator, securityContext, bridge );
        facade.init( procedureGDBFacadeSPI, bridge, platform.config, tokenHolders );
        return facade;
    }
}
