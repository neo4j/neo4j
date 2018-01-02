/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.perftest.enterprise.ccheck;

import java.io.File;
import java.util.Map;

import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.index.lucene.LuceneLabelScanStoreBuilder;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.impl.index.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.perftest.enterprise.generator.DataGenerator;
import org.neo4j.perftest.enterprise.util.Configuration;
import org.neo4j.perftest.enterprise.util.Parameters;
import org.neo4j.perftest.enterprise.util.Setting;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static org.neo4j.consistency.ConsistencyCheckService.defaultConsistencyCheckThreadsNumber;
import static org.neo4j.perftest.enterprise.util.Configuration.SYSTEM_PROPERTIES;
import static org.neo4j.perftest.enterprise.util.Configuration.settingsOf;
import static org.neo4j.perftest.enterprise.util.DirectlyCorrelatedParameter.param;
import static org.neo4j.perftest.enterprise.util.DirectlyCorrelatedParameter.passOn;
import static org.neo4j.perftest.enterprise.util.Setting.booleanSetting;
import static org.neo4j.perftest.enterprise.util.Setting.enumSetting;
import static org.neo4j.perftest.enterprise.util.Setting.stringSetting;

public class ConsistencyPerformanceCheck
{
    static final Setting<Boolean> generate_graph = booleanSetting( "generate_graph", false );
    static final Setting<String> report_file = stringSetting( "report_file", "target/report.json" );
    static final Setting<CheckerVersion> checker_version = enumSetting( "checker_version", CheckerVersion.NEW );
    static final Setting<Boolean> wait_before_check = booleanSetting( "wait_before_check", false );
    static final Setting<String> pagecache_memory =
            stringSetting( "dbms.pagecache.memory", "2G" );
    static final Setting<String> mapped_memory_page_size = stringSetting( "dbms.pagecache.pagesize", "4k" );
    private static PageCache pageCache;
    private static FileSystemAbstraction fileSystem;

    /**
     * Sample execution:
     * java -cp ... org.neo4j.perftest.enterprise.ccheck.ConsistencyPerformanceCheck
     * -generate_graph
     * -report_file target/ccheck_performance.json
     * -neo4j.store_dir target/ccheck_perf_graph
     * -report_progress
     * -node_count 10000000
     * -relationships FOO:2,BAR:1
     * -node_properties INTEGER:2,STRING:1,BYTE_ARRAY:1
     */
    public static void main( String... args ) throws Exception
    {
        run( Parameters.configuration( SYSTEM_PROPERTIES,
                settingsOf( DataGenerator.class, ConsistencyPerformanceCheck.class ) )
                .convert( args ) );
    }

    private static void run( Configuration configuration ) throws Exception
    {
        if ( configuration.get( generate_graph ) )
        {
            DataGenerator.run( configuration );
        }

        File storeDir = new File( configuration.get( DataGenerator.store_dir ) );

        // ensure that the store is recovered
        new GraphDatabaseFactory().newEmbeddedDatabase( storeDir ).shutdown();

        // run the consistency check
        ProgressMonitorFactory progress;
        if ( configuration.get( DataGenerator.report_progress ) )
        {
            progress = ProgressMonitorFactory.textual( System.out );
        }
        else
        {
            progress = ProgressMonitorFactory.NONE;
        }
        if ( configuration.get( wait_before_check ) )
        {
            System.out.println( "Press return to start the checker..." );
            System.in.read();
        }

        Config tuningConfiguration = buildTuningConfiguration( configuration );
        fileSystem = new DefaultFileSystemAbstraction();
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, tuningConfiguration, PageCacheTracer.NULL, NullLog.getInstance() );
        pageCache = pageCacheFactory.getOrCreatePageCache();
        DirectStoreAccess directStoreAccess = createScannableStores( storeDir, tuningConfiguration );

        JsonReportWriter reportWriter = new JsonReportWriter( configuration, tuningConfiguration );
        TimingProgress progressMonitor = new TimingProgress( new TimeLogger( reportWriter ), progress );

        try
        {
            configuration.get( checker_version ).run( progressMonitor, directStoreAccess, tuningConfiguration,
                    Statistics.NONE, defaultConsistencyCheckThreadsNumber() );
        }
        finally
        {
            directStoreAccess.close();
            pageCache.close();
        }
    }

    private static DirectStoreAccess createScannableStores( File storeDir, Config tuningConfiguration )
    {
        StoreFactory factory = new StoreFactory( storeDir, tuningConfiguration,
                new DefaultIdGeneratorFactory( fileSystem ), pageCache, fileSystem, NullLogProvider.getInstance() );
        NeoStores neoStores = factory.openAllNeoStores( true );
        OperationalMode operationalMode = OperationalMode.single;
        SchemaIndexProvider indexes = new LuceneSchemaIndexProvider(
                fileSystem,
                DirectoryFactory.PERSISTENT,
                storeDir, NullLogProvider.getInstance(), tuningConfiguration, operationalMode );
        LuceneLabelScanStoreBuilder labelScanStoreBuilder = new LuceneLabelScanStoreBuilder( storeDir, neoStores,
                fileSystem, tuningConfiguration, operationalMode, NullLogProvider.getInstance() );
        LabelScanStore labelScanStore = labelScanStoreBuilder.build();
        return new DirectStoreAccess( new StoreAccess( neoStores ).initialize(), labelScanStore, indexes );
    }

    private static Config buildTuningConfiguration( Configuration configuration )
    {
        Map<String, String> passedOnConfiguration = passOn( configuration,
                param( GraphDatabaseSettings.pagecache_memory, pagecache_memory ),
                param( GraphDatabaseSettings.mapped_memory_page_size, mapped_memory_page_size ) );
        return new Config( passedOnConfiguration, GraphDatabaseSettings.class );
    }
}
