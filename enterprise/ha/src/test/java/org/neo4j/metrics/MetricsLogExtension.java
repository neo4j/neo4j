/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.metrics;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;

public class MetricsLogExtension implements Lifecycle
{
    private Monitors monitors;
    private Config config;
    private FileSystemAbstraction fileSystemAbstraction;
    private TxManager txManager;
    private ByteCounterMetrics networkCounterMetrics;
    private ByteCounterMetrics diskCounterMetrics;
    private ScheduledExecutorService executor;
    private CSVFile csv;

    public MetricsLogExtension( Monitors monitors, Config config, FileSystemAbstraction fileSystemAbstraction, TxManager txManager )
    {
        this.monitors = monitors;
        this.config = config;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.txManager = txManager;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        networkCounterMetrics = new ByteCounterMetrics();
        monitors.addMonitorListener( networkCounterMetrics, MasterServer.class.getName() );
        monitors.addMonitorListener( networkCounterMetrics, "logdeserializer" );

        diskCounterMetrics = new ByteCounterMetrics();
        monitors.addMonitorListener( diskCounterMetrics, XaLogicalLog.class.getName() );

        File path = new File( config.get( GraphDatabaseSettings.store_dir ), "metrics.txt" );
        System.out.println("CSV:"+path);

        OutputStream file = fileSystemAbstraction.openAsOutputStream( path, false );

        csv = new CSVFile( file, Iterables.<String, String>iterable( "timestamp", "diskWritten", "diskRead", "networkWritten", "networkRead", "committedTx" ) );

        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    csv.print( System.currentTimeMillis(),
                            diskCounterMetrics.getBytesWritten(), diskCounterMetrics.getBytesRead(),
                            networkCounterMetrics.getBytesWritten(), networkCounterMetrics.getBytesRead(),
                            txManager.getCommittedTxCount());
                    System.out.println( config.get( ClusterSettings.server_id ) + " bytes written:" + networkCounterMetrics
                            .getBytesWritten() );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }, 1, 1, TimeUnit.SECONDS );
    }

    @Override
    public void stop() throws Throwable
    {
        executor.shutdown();
        if (!executor.awaitTermination( 10, TimeUnit.SECONDS ))
            executor.shutdownNow();

        csv.close();

        monitors.removeMonitorListener( networkCounterMetrics );
    }

    @Override
    public void shutdown() throws Throwable
    {
    }
}
