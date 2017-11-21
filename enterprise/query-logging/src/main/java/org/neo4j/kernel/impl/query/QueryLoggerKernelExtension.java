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
package org.neo4j.kernel.impl.query;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

@Service.Implementation( KernelExtensionFactory.class )
public class QueryLoggerKernelExtension extends KernelExtensionFactory<QueryLoggerKernelExtension.Dependencies>
{
    public interface Dependencies
    {
        FileSystemAbstraction fileSystem();

        Config config();

        Monitors monitoring();

        LogService logger();

        JobScheduler jobScheduler();
    }

    public QueryLoggerKernelExtension()
    {
        super( "query-logging" );
    }

    @Override
    public Lifecycle newInstance( @SuppressWarnings( "unused" ) KernelContext context, final Dependencies dependencies ) throws Throwable
    {
        FileSystemAbstraction fileSystem = dependencies.fileSystem();
        Config config = dependencies.config();
        Monitors monitoring = dependencies.monitoring();
        LogService logService = dependencies.logger();
        JobScheduler jobScheduler = dependencies.jobScheduler();

        return new LifecycleAdapter()
        {
            DynamicLoggingQueryExecutionMonitor logger;

            @Override
            public void init() throws Throwable
            {
                Log debugLog = logService.getInternalLog( DynamicLoggingQueryExecutionMonitor.class );
                this.logger = new DynamicLoggingQueryExecutionMonitor( config, fileSystem, jobScheduler, debugLog );
                this.logger.init();
                monitoring.addMonitorListener( this.logger );
            }

            @Override
            public void shutdown() throws Throwable
            {
                logger.close();
            }
        };
    }
}
