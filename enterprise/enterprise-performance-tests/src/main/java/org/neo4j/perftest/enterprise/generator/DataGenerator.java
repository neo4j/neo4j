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
package org.neo4j.perftest.enterprise.generator;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.logging.NullLog;
import org.neo4j.perftest.enterprise.util.Configuration;
import org.neo4j.perftest.enterprise.util.Conversion;
import org.neo4j.perftest.enterprise.util.Parameters;
import org.neo4j.perftest.enterprise.util.Setting;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static java.util.Arrays.asList;

import static org.neo4j.perftest.enterprise.util.Configuration.SYSTEM_PROPERTIES;
import static org.neo4j.perftest.enterprise.util.Configuration.settingsOf;
import static org.neo4j.perftest.enterprise.util.Predicate.integerRange;
import static org.neo4j.perftest.enterprise.util.Setting.adaptSetting;
import static org.neo4j.perftest.enterprise.util.Setting.booleanSetting;
import static org.neo4j.perftest.enterprise.util.Setting.integerSetting;
import static org.neo4j.perftest.enterprise.util.Setting.listSetting;
import static org.neo4j.perftest.enterprise.util.Setting.restrictSetting;
import static org.neo4j.perftest.enterprise.util.Setting.stringSetting;

public class DataGenerator
{
    public static final Setting<String> store_dir = stringSetting( "neo4j.store_dir", "target/generated-data/graph.db" );
    public static final Setting<Boolean> report_progress = booleanSetting( "report_progress", false );
    static final Setting<Boolean> report_stats = booleanSetting( "report_stats", false );
    static final Setting<Integer> node_count = adaptSetting(
            restrictSetting( integerSetting( "node_count", 100 ), integerRange( 0, Integer.MAX_VALUE ) ),
            Conversion.TO_INTEGER );
    static final Setting<List<RelationshipSpec>> relationships = listSetting(
            adaptSetting( stringSetting( "relationships" ), RelationshipSpec.FROM_STRING ),
            asList( new RelationshipSpec( "RELATED_TO", 2 ) ) );
    static final Setting<List<PropertySpec>> node_properties = listSetting(
            adaptSetting( Setting.stringSetting( "node_properties" ), PropertySpec.PARSER ),
            asList( new PropertySpec( PropertyGenerator.STRING, 1 ),
                    new PropertySpec( PropertyGenerator.BYTE_ARRAY, 1 ) ) );
    static final Setting<List<PropertySpec>> relationship_properties = listSetting(
            adaptSetting( Setting.stringSetting( "relationship_properties" ), PropertySpec.PARSER ),
            Collections.<PropertySpec>emptyList() );
    private static final Setting<String> pagecache_memory =
            stringSetting( "dbms.pagecache.memory", "2G" );

    public static final Random RANDOM = new Random();
    private final boolean reportProgress;
    private final int nodeCount;
    private final int relationshipCount;
    private final List<RelationshipSpec> relationshipsForEachNode;
    private final List<PropertySpec> nodeProperties;
    private final List<PropertySpec> relationshipProperties;

    public static void main( String... args ) throws Exception
    {
        run( Parameters.configuration( SYSTEM_PROPERTIES, settingsOf( DataGenerator.class ) ).convert( args ) );
    }

    public DataGenerator( Configuration configuration )
    {
        this.reportProgress = configuration.get( report_progress );
        this.nodeCount = configuration.get( node_count );
        this.relationshipsForEachNode = configuration.get( relationships );
        this.nodeProperties = configuration.get( node_properties );
        this.relationshipProperties = configuration.get( relationship_properties );
        int relCount = 0;
        for ( RelationshipSpec rel : relationshipsForEachNode )
        {
            relCount += rel.count;
        }
        this.relationshipCount = nodeCount * relCount;
    }

    public static void run( Configuration configuration ) throws IOException
    {
        File storeDir = new File( configuration.get( store_dir ) );
        FileUtils.deleteRecursively( storeDir );
        DataGenerator generator = new DataGenerator( configuration );
        Map<String,String> config = batchInserterConfig( configuration );
        BatchInserter batchInserter = BatchInserters.inserter( storeDir.getAbsoluteFile(), config );
        try
        {
            generator.generateData( batchInserter );
        }
        finally
        {
            batchInserter.shutdown();
        }
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                new DefaultFileSystemAbstraction(), new Config( config ), PageCacheTracer.NULL,
                NullLog.getInstance() );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();
        StoreAccess stores = new StoreAccess( pageCache, storeDir ).initialize();
        try
        {
            printCount( stores.getNodeStore() );
            printCount( stores.getRelationshipStore() );
            printCount( stores.getPropertyStore() );
            printCount( stores.getStringStore() );
            printCount( stores.getArrayStore() );
            if ( configuration.get( report_stats ) )
            {
                PropertyStats stats = new PropertyStats();
                stats.applyFiltered( stores.getPropertyStore() );
                // TODO please pass in the PrintStream as an argument
                System.out.println( stats );
            }
        }
        finally
        {
            stores.close();
            pageCache.flushAndForce();
            pageCache.close();
        }
    }

    public void generateData( BatchInserter batchInserter )
    {
        ProgressMonitorFactory.MultiPartBuilder builder = initProgress();
        ProgressListener nodeProgressListener = builder.progressForPart( "nodes", nodeCount );
        ProgressListener relationshipsProgressListener = builder.progressForPart( "relationships", relationshipCount );
        builder.build();

        generateNodes( batchInserter, nodeProgressListener );
        generateRelationships( batchInserter, relationshipsProgressListener );
    }

    @Override
    public String toString()
    {
        return "DataGenerator{" +
                "nodeCount=" + nodeCount +
                ", relationshipCount=" + relationshipCount +
                ", relationshipsForEachNode=" + relationshipsForEachNode +
                ", nodeProperties=" + nodeProperties +
                ", relationshipProperties=" + relationshipProperties +
                '}';
    }

    private void generateNodes( BatchInserter batchInserter, ProgressListener progressListener )
    {
        for ( int i = 0; i < nodeCount; i++ )
        {
            batchInserter.createNode( generate( nodeProperties ) );
            progressListener.set( i );
        }
        progressListener.done();
    }

    private void generateRelationships( BatchInserter batchInserter, ProgressListener progressListener )
    {
        for ( int i = 0; i < nodeCount; i++ )
        {
            for ( RelationshipSpec relationshipSpec : relationshipsForEachNode )
            {
                for ( int j = 0; j < relationshipSpec.count; j++ )
                {
                    batchInserter.createRelationship( i, RANDOM.nextInt( nodeCount ), relationshipSpec,
                                                      generate( relationshipProperties ) );
                    progressListener.add( 1 );
                }
            }
        }
        progressListener.done();
    }

    protected ProgressMonitorFactory.MultiPartBuilder initProgress()
    {
        return (reportProgress ? ProgressMonitorFactory.textual( System.out ) : ProgressMonitorFactory.NONE)
                .multipleParts( "Generating " + this );
    }

    private Map<String, Object> generate( List<PropertySpec> properties )
    {
        Map<String, Object> result = new HashMap<>();
        for ( PropertySpec property : properties )
        {
            result.putAll( property.generate() );
        }
        return result;
    }

    private static void printCount( RecordStore<?> store )
    {
        File name = store.getStorageFileName();
        System.out.format( "Number of records in %s: %d%n", name.getName(), store.getHighId() );
    }

    private static Map<String, String> batchInserterConfig( Configuration configuration )
    {
        Map<String, String> config = new HashMap<>();
        config.put( "dump_configuration", "true" );
        config.put( GraphDatabaseSettings.pagecache_memory.name(),
                configuration.get( pagecache_memory ) );
        return config;
    }

}
