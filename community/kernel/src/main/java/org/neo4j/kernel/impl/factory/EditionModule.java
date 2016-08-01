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
package org.neo4j.kernel.impl.factory;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.internal.KernelDiagnostics;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static java.util.Collections.singletonMap;

/**
 * Edition module for {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory}. Implementations of this class
 * need to create all the services that would be specific for a particular edition of the database.
 */
public abstract class EditionModule
{
    public void registerProcedures( Procedures procedures )
    {
        // do nothing
    }

    public IdGeneratorFactory idGeneratorFactory;
    public IdTypeConfigurationProvider idTypeConfigurationProvider;

    public LabelTokenHolder labelTokenHolder;

    public PropertyKeyTokenHolder propertyKeyTokenHolder;

    public Locks lockManager;

    public StatementLocksFactory statementLocksFactory;

    public CommitProcessFactory commitProcessFactory;

    public long transactionStartTimeout;

    public RelationshipTypeTokenHolder relationshipTypeTokenHolder;

    public TransactionHeaderInformationFactory headerInformationFactory;

    public SchemaWriteGuard schemaWriteGuard;

    public ConstraintSemantics constraintSemantics;

    public CoreAPIAvailabilityGuard coreAPIAvailabilityGuard;

    public IOLimiter ioLimiter;

    public IdReuseEligibility eligibleForIdReuse;

    protected void doAfterRecoveryAndStartup( DatabaseInfo databaseInfo, DependencyResolver dependencyResolver )
    {
        DiagnosticsManager diagnosticsManager = dependencyResolver.resolveDependency( DiagnosticsManager.class );
        NeoStoreDataSource neoStoreDataSource = dependencyResolver.resolveDependency( NeoStoreDataSource.class );

        diagnosticsManager.prependProvider( new KernelDiagnostics.Versions(
                databaseInfo, neoStoreDataSource.getStoreId() ) );
        neoStoreDataSource.registerDiagnosticsWith( diagnosticsManager );
        diagnosticsManager.appendProvider( new KernelDiagnostics.StoreFiles( neoStoreDataSource.getStoreDir() ) );
    }

    protected void publishEditionInfo( UsageData sysInfo, DatabaseInfo databaseInfo, Config config )
    {
        sysInfo.set( UsageDataKeys.edition, databaseInfo.edition );
        sysInfo.set( UsageDataKeys.operationalMode, databaseInfo.operationalMode );
        config.augment( singletonMap( Configuration.editionName.name(), databaseInfo.edition.toString() ) );
    }

    public static AuthManager createAuthManager( Config config, LogService logging )
    {
        boolean authEnabled = config.get( GraphDatabaseSettings.auth_enabled );
        if ( !authEnabled )
        {
            return AuthManager.NO_AUTH;
        }

        String key = config.get( GraphDatabaseSettings.auth_manager );
        for ( AuthManager.Factory candidate : Service.load( AuthManager.Factory.class ) )
        {
            String candidateId = candidate.getKeys().iterator().next();
            if ( candidateId.equals( key ) )
            {
                return candidate.newInstance( config, logging.getUserLogProvider() );
            }
            else if ( key.isEmpty() )
            {
                // As a default use the available service for the configured build edition
                logging.getInternalLog( GraphDatabaseFacadeFactory.class )
                        .info( "No auth manager implementation specified, defaulting to '" + candidateId + "'" );
                return candidate.newInstance( config, logging.getUserLogProvider() );
            }
        }

        if ( key.isEmpty() )
        {
            logging.getUserLog( GraphDatabaseFacadeFactory.class )
                    .error( "No auth manager implementation specified and no default could be loaded. " +
                            "It is an illegal product configuration to have auth enabled and not provide an " +
                            "auth manager service." );
            throw new IllegalArgumentException( "Auth enabled but no auth manager found. This is an illegal product configuration." );
        }

        throw new IllegalArgumentException( "No auth manager found with the name '" + key + "'." );
    }

    protected BoltConnectionTracker createSessionTracker()
    {
        return BoltConnectionTracker.NOOP;
    }
}
