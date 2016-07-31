/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.storecopy;

import java.io.File;

import org.neo4j.com.storecopy.ExternallyManagedPageCache;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.logging.NullLogProvider;

public class CopiedStoreRecovery
{
    private final Config config;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final PageCache pageCache;

    public CopiedStoreRecovery( Config config, Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                PageCache pageCache )
    {
        this.config = config;
        this.kernelExtensions = kernelExtensions;
        this.pageCache = pageCache;
    }

    void recoverCopiedStore( File tempStore )
    {
        try
        {
            GraphDatabaseService graphDatabaseService = newTempDatabase( tempStore );
            graphDatabaseService.shutdown();
        }
        catch ( Exception e )
        {
            if ( e.getCause() != null && e.getCause().getCause() instanceof UpgradeNotAllowedByConfigurationException )
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

        return String.format( "Failed to start database with copied store. This may be because the core and edge " +
                        "servers have a different record format. On this machine: `%s=%s`. Check the equivalent value" +
                " on the core server.",
                GraphDatabaseSettings.record_format.name(), recordFormat );
    }

    private GraphDatabaseService newTempDatabase( File tempStore )
    {
        ExternallyManagedPageCache.GraphDatabaseFactoryWithPageCacheFactory factory =
                ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache( pageCache );
        return factory
                .setKernelExtensions( kernelExtensions )
                .setUserLogProvider( NullLogProvider.getInstance() )
                .newEmbeddedDatabaseBuilder( tempStore )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade,
                        config.get( GraphDatabaseSettings.allow_store_upgrade ).toString() )
                .setConfig( GraphDatabaseSettings.record_format,
                        config.get( GraphDatabaseSettings.record_format ) )
                .newGraphDatabase();
    }
}
