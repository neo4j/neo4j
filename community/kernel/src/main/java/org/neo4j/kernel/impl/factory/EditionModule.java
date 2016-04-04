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

import java.io.File;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.NeoStoreDataSource;
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
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.internal.KernelDiagnostics;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.auth.AuthManager;
import org.neo4j.server.security.auth.BasicAuthManager;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static java.time.Clock.systemUTC;
import static java.util.Collections.singletonMap;

/**
 * Edition module for {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory}. Implementations of this class
 * need to create all the services that would be specific for a particular edition of the database.
 */
public abstract class EditionModule
{
    public IdGeneratorFactory idGeneratorFactory;

    public LabelTokenHolder labelTokenHolder;

    public PropertyKeyTokenHolder propertyKeyTokenHolder;

    public Locks lockManager;

    public CommitProcessFactory commitProcessFactory;

    public long transactionStartTimeout;

    public RelationshipTypeTokenHolder relationshipTypeTokenHolder;

    public TransactionHeaderInformationFactory headerInformationFactory;

    public SchemaWriteGuard schemaWriteGuard;

    public ConstraintSemantics constraintSemantics;

    public CoreAPIAvailabilityGuard coreAPIAvailabilityGuard;

    public IOLimiter ioLimiter;

    public RecordFormats formats;

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

    protected AuthManager createAuthManager( Config config, LifeSupport life, LogProvider logProvider )
    {
        boolean authEnabled = config.get( GraphDatabaseSettings.auth_enabled );
        if ( authEnabled )
        {
            File storePath = config.get( GraphDatabaseSettings.auth_store );
            if ( storePath == null )
            {
                logProvider.getLog( EditionModule.class ).warn( "Authentication not enabled because %s is not set.",
                        GraphDatabaseSettings.auth_store.name() );
                return AuthManager.NO_AUTH;
            }
            FileUserRepository users = life.add( new FileUserRepository( storePath.toPath(), logProvider ) );
            return life.add( new BasicAuthManager( users, systemUTC(), true ) );
        }
        else
        {
            return AuthManager.NO_AUTH;
        }

    }
}
