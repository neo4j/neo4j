/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInProcedures;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.stats.IdBasedStoreEntityCounters;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;

/**
 * This implementation of {@link EditionModule} creates the implementations of services
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
        platformModule.dependencies.satisfyDependency( new IdBasedStoreEntityCounters( this.idGeneratorFactory ) );
        ioLimiter = new ConfigurableIOLimiter( platformModule.config );
        platformModule.dependencies.satisfyDependency( createSessionTracker() );
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
    protected IdTypeConfigurationProvider createIdTypeConfigurationProvider( Config config )
    {
        return new EnterpriseIdTypeConfigurationProvider( config );
    }

    @Override
    protected ConstraintSemantics createSchemaRuleVerifier()
    {
        return new EnterpriseConstraintSemantics();
    }

    @Override
    protected BoltConnectionTracker createSessionTracker()
    {
        return new StandardBoltConnectionTracker();
    }

    @Override
    protected StatementLocksFactory createStatementLocksFactory( Locks locks, Config config, LogService logService )
    {
        return new StatementLocksFactorySelector( locks, config, logService ).select();
    }

    @Override
    public void setupSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        EnterpriseEditionModule.setupEnterpriseSecurityModule( platformModule, procedures );
    }

    public static void setupEnterpriseSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        if ( platformModule.config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            setupSecurityModule( platformModule,
                    platformModule.logging.getUserLog( EnterpriseEditionModule.class ),
                    procedures, platformModule.config.get( EnterpriseEditionSettings.security_module ) );
        }
        else
        {
            platformModule.life.add( platformModule.dependencies.satisfyDependency( EnterpriseAuthManager.NO_AUTH ) );
            platformModule.life.add( platformModule.dependencies.satisfyDependency( UserManagerSupplier.NO_AUTH ) );
        }
    }
}
