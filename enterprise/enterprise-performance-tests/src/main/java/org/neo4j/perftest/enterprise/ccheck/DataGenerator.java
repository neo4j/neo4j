/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static java.util.Arrays.asList;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.NumberOfBytesSetting.parseNumberOfBytes;
import static org.neo4j.perftest.enterprise.util.Configuration.SYSTEM_PROPERTIES;
import static org.neo4j.perftest.enterprise.util.Configuration.settingsOf;
import static org.neo4j.perftest.enterprise.util.Predicate.integerRange;
import static org.neo4j.perftest.enterprise.util.Setting.adaptSetting;
import static org.neo4j.perftest.enterprise.util.Setting.booleanSetting;
import static org.neo4j.perftest.enterprise.util.Setting.integerSetting;
import static org.neo4j.perftest.enterprise.util.Setting.listSetting;
import static org.neo4j.perftest.enterprise.util.Setting.restrictSetting;
import static org.neo4j.perftest.enterprise.util.Setting.stringSetting;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.perftest.enterprise.util.Configuration;
import org.neo4j.perftest.enterprise.util.Conversion;
import org.neo4j.perftest.enterprise.util.Parameters;
import org.neo4j.perftest.enterprise.util.Setting;

public class DataGenerator
{
    public static final Random RANDOM = new Random();

    enum PropertyGenerator
    {
        INTEGER
                {
                    @Override
                    Object generate()
                    {
                        return RANDOM.nextInt( 16 );
                    }
                },
        SINGLE_STRING
                {
                    @Override
                    Object generate()
                    {
                        return name();
                    }
                },
        STRING
                {
                    @Override
                    Object generate()
                    {
                        int length = 50 + RANDOM.nextInt( 70 );
                        StringBuilder result = new StringBuilder( length );
                        for ( int i = 0; i < length; i++ )
                        {
                            result.append( (char) ('a' + RANDOM.nextInt( 'z' - 'a' )) );
                        }
                        return result.toString();
                    }
                },
        BYTE_ARRAY
                {
                    @Override
                    Object generate()
                    {
//                        int length = 4 + RANDOM.nextInt( 60 );
                        int length = 50;
                        int[] array = new int[length];
                        for ( int i = 0; i < length; i++ )
                        {
                            array[i] = RANDOM.nextInt( 256 );
                        }
                        return array;
                    }
                };

        abstract Object generate();
    }

    static class PropertySpec
    {
        public static final Conversion<String,PropertySpec> PARSER = new Conversion<String, PropertySpec>()
        {
            @Override
            public PropertySpec convert( String value )
            {
                String[] tokens = value.split( ":" );
                return new PropertySpec( PropertyGenerator.valueOf( tokens[0] ), Float.parseFloat( tokens[1] ) );
            }
        };

        private final PropertyGenerator propertyGenerator;
        private final float count;

        PropertySpec( PropertyGenerator propertyGenerator, float count )
        {
            this.propertyGenerator = propertyGenerator;
            this.count = count;
        }

        public Map<String, Object> generate()
        {
            HashMap<String, Object> map = new HashMap<String, Object>();
            int propertyCount = (int) count;
            if (RANDOM.nextFloat() < count - propertyCount)
            {
                propertyCount++;
            }
            for ( int i = 0; i < propertyCount; i++ )
            {
                map.put( propertyGenerator.name() + "_" + i, propertyGenerator.generate() );
            }
            return map;
        }

        @Override
        public String toString()
        {
            return propertyGenerator.name() + ":" + count;
        }
    }

    static class RelationshipSpec implements RelationshipType
    {
        static final Conversion<String, RelationshipSpec> FROM_STRING = new Conversion<String, RelationshipSpec>()
        {
            @Override
            public RelationshipSpec convert( String source )
            {
                String[] parts = source.split( ":" );
                if ( parts.length != 2 )
                {
                    throw new IllegalArgumentException( "value must have the format <relationship label>:<count>" );
                }
                return new RelationshipSpec( parts[0], Integer.parseInt( parts[1] ) );
            }
        };
        private final String name;
        private final int count;

        public RelationshipSpec( String name, int count )
        {
            this.name = name;
            this.count = count;
        }

        @Override
        public String toString()
        {
            return name() + ":" + count;
        }

        @Override
        public String name()
        {
            return name;
        }
    }

    static final Setting<String> store_dir = stringSetting( "neo4j.store_dir", "target/generated-data/graph.db" );
    static final Setting<Boolean> report_progress = booleanSetting( "report_progress", false );
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
    private static final Setting<String> all_stores_total_mapped_memory_size =
            stringSetting( "all_stores_total_mapped_memory_size", "2G" );

    private final boolean reportProgress;
    private final int nodeCount;
    private final int relationshipCount;
    private final List<RelationshipSpec> relationshipsForEachNode;
    private final List<PropertySpec> nodeProperties;
    private final List<PropertySpec> relationshipProperties;

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

    public void generateData( BatchInserter batchInserter )
    {
        ProgressMonitorFactory.MultiPartBuilder builder = initProgress();
        ProgressListener nodeProgressListener = builder.progressForPart( "nodes", nodeCount );
        ProgressListener relationshipsProgressListener = builder.progressForPart( "relationships", relationshipCount );
        builder.build();

        generateNodes( batchInserter, nodeProgressListener );
        generateRelationships( batchInserter, relationshipsProgressListener );
    }

    private void generateNodes( BatchInserter batchInserter, ProgressListener progressListener )
    {
        batchInserter.setNodeProperties( 0, generate( nodeProperties ) ); // reference node properties
        for ( int i = 1 /*reference node already exists*/; i < nodeCount; i++ )
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
        Map<String, Object> result = new HashMap<String, Object>();
        for ( PropertySpec property : properties )
        {
            result.putAll( property.generate() );
        }
        return result;
    }

    public static void main( String... args ) throws Exception
    {
        run( Parameters.configuration( SYSTEM_PROPERTIES, settingsOf( DataGenerator.class ) ).convert( args ) );
    }

    static void run( Configuration configuration ) throws IOException
    {
        String storeDir = configuration.get( store_dir );
        FileUtils.deleteRecursively( new File( storeDir ) );
        DataGenerator generator = new DataGenerator( configuration );
        BatchInserter batchInserter = new BatchInserterImpl( storeDir, batchInserterConfig( configuration ) );
        try
        {
            generator.generateData( batchInserter );
        }
        finally
        {
            batchInserter.shutdown();
        }
        StoreAccess stores = new StoreAccess( storeDir );
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
                stats.applyFiltered( stores.getPropertyStore(), RecordStore.IN_USE );
                System.out.println( stats );
            }
        }
        finally
        {
            stores.close();
        }
    }

    private static void printCount( RecordStore<?> store )
    {
        String name = store.getStorageFileName();
        name = name.substring( name.lastIndexOf( '/' ) + 1 );
        System.out.format( "Number of records in %s: %d%n", name, store.getHighId() );
    }

    private static Map<String, String> batchInserterConfig(Configuration configuration)
    {
        Long mappedMemory = parseNumberOfBytes( configuration.get( all_stores_total_mapped_memory_size ) );

        Map<String, String> config = new HashMap<String, String>();
        config.put( "use_memory_mapped_buffers", "true" );
        config.put( "dump_configuration", "true" );
        config.put( "neostore.nodestore.db.mapped_memory", mega( mappedMemory / 6 ) );
        config.put( "neostore.relationshipstore.db.mapped_memory", mega( mappedMemory / 2 ) );
        config.put( "neostore.propertystore.db.mapped_memory", mega( mappedMemory / 3 ) );
        config.put( GraphDatabaseSettings.all_stores_total_mapped_memory_size.name(),
                configuration.get( all_stores_total_mapped_memory_size ) );

        return config;
    }

    private static String mega( long bytes )
    {
        return (bytes / 1024 / 1024) + "M";
    }

    private static class PropertyStats extends RecordStore.Processor
    {
        Map<Integer, Long> sizeHistogram = new TreeMap<Integer, Long>();
        Map<PropertyType, Long> typeHistogram = new EnumMap<PropertyType, Long>( PropertyType.class );

        @Override
        public void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property )
        {
            List<PropertyBlock> blocks = property.getPropertyBlocks();
            update( sizeHistogram, blocks.size() );
            for ( PropertyBlock block : blocks )
            {
                update( typeHistogram, block.getType() );
            }
        }

        private <T> void update( Map<T, Long> histogram, T key )
        {
            Long value = histogram.get( key );
            histogram.put( key, (value == null) ? 1 : (value + 1) );
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder( getClass().getSimpleName() ).append( "{\n" );
            for ( Map.Entry<Integer, Long> entry : sizeHistogram.entrySet() )
            {
                builder.append( '\t' ).append( entry.getKey() ).append( ": " ).append( entry.getValue() ).append(
                        '\n' );
            }
            for ( Map.Entry<PropertyType, Long> entry : typeHistogram.entrySet() )
            {
                builder.append( '\t' ).append( entry.getKey() ).append( ": " ).append( entry.getValue() ).append(
                        '\n' );
            }
            return builder.append( '}' ).toString();
        }
    }
}
