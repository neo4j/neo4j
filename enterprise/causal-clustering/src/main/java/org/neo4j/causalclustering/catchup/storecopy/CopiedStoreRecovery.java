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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.com.storecopy.ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache;

public class CopiedStoreRecovery extends LifecycleAdapter
{
    private final Config config;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final PageCache pageCache;

    private boolean shutdown;

    public CopiedStoreRecovery( Config config, Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                PageCache pageCache )
    {
        this.config = config;
        this.kernelExtensions = kernelExtensions;
        this.pageCache = pageCache;
    }

    @Override
    public synchronized void shutdown()
    {
        shutdown = true;
    }

    public synchronized void recoverCopiedStore( File tempStore ) throws DatabaseShutdownException
    {
        if ( shutdown )
        {
            throw new DatabaseShutdownException( "Abort store-copied store recovery due to database shutdown" );
        }

        try
        {
            GraphDatabaseService graphDatabaseService = newTempDatabase( tempStore );
            graphDatabaseService.shutdown();
        }
        catch ( Exception e )
        {
            Throwable peeled = Exceptions.peel( e, t -> !(t instanceof UpgradeNotAllowedByConfigurationException) );
            if ( peeled != null )
            {
                throw new RuntimeException( failedToStartMessage(), e );
            }
            else
            {
                throw e;
            }
        }
    }

    private String failedToStartMessage()
    {
        String recordFormat = config.get( GraphDatabaseSettings.record_format );

        return String.format( "Failed to start database with copied store. This may be because the core servers and " +
                        "read replicas have a different record format. On this machine: `%s=%s`. Check the equivalent" +
                        " value on the core server.",
                GraphDatabaseSettings.record_format.name(), recordFormat );
    }

    private GraphDatabaseService newTempDatabase( File tempStore )
    {
        return graphDatabaseFactoryWithPageCache( pageCache )
                .setKernelExtensions( kernelExtensions )
                .setUserLogProvider( NullLogProvider.getInstance() )
                .newEmbeddedDatabaseBuilder( tempStore )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.pagecache_warmup_enabled, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.allow_upgrade,
                        config.get( GraphDatabaseSettings.allow_upgrade ).toString() )
                .setConfig( GraphDatabaseSettings.record_format, config.get( GraphDatabaseSettings.record_format ) )
                .newGraphDatabase();
    }
}
