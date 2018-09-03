/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.enterprise;

import java.util.function.Predicate;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.api.security.provider.EnterpriseNoAuthSecurityProvider;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInProcedures;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.logging.internal.LogService;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Enterprise edition, without HA
 */
public class EnterpriseEditionModule extends CommunityEditionModule
{
    @Override
    public void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException
    {
        procedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        procedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
    }

    public EnterpriseEditionModule( PlatformModule platformModule )
    {
        super( platformModule );
        ioLimiter = new ConfigurableIOLimiter( platformModule.config );
    }

    @Override
    protected IdContextFactory createIdContextFactory( PlatformModule platformModule, FileSystemAbstraction fileSystem )
    {
        return IdContextFactoryBuilder.of( new EnterpriseIdTypeConfigurationProvider( platformModule.config ), platformModule.jobScheduler )
                .withFileSystem( fileSystem )
                .build();
    }

    @Override
    protected Predicate<String> fileWatcherFileNameFilter()
    {
        return enterpriseNonClusterFileWatcherFileNameFilter();
    }

    static Predicate<String> enterpriseNonClusterFileWatcherFileNameFilter()
    {
        return Predicates.any(
                fileName -> fileName.startsWith( TransactionLogFiles.DEFAULT_NAME ),
                fileName -> fileName.startsWith( IndexConfigStore.INDEX_DB_FILE_NAME ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF )
        );
    }

    @Override
    protected ConstraintSemantics createSchemaRuleVerifier()
    {
        return new EnterpriseConstraintSemantics();
    }

    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    @Override
    protected StatementLocksFactory createStatementLocksFactory( Locks locks, Config config, LogService logService )
    {
        return new StatementLocksFactorySelector( locks, config, logService ).select();
    }

    @Override
    public void createSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        EnterpriseEditionModule.createEnterpriseSecurityModule( this, platformModule, procedures );
    }

    public static void createEnterpriseSecurityModule( AbstractEditionModule editionModule, PlatformModule platformModule, Procedures procedures )
    {
        SecurityProvider securityProvider;
        if ( platformModule.config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            SecurityModule securityModule = setupSecurityModule( platformModule, editionModule,
                    platformModule.logging.getUserLog( EnterpriseEditionModule.class ),
                    procedures, platformModule.config.get( EnterpriseEditionSettings.security_module ) );
            platformModule.life.add( securityModule );
            securityProvider = securityModule;
        }
        else
        {
            EnterpriseNoAuthSecurityProvider provider = EnterpriseNoAuthSecurityProvider.INSTANCE;
            platformModule.life.add( provider );
            securityProvider = provider;
        }
        editionModule.setSecurityProvider( securityProvider );
    }
}
