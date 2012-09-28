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

import static org.neo4j.perftest.enterprise.util.Configuration.SYSTEM_PROPERTIES;
import static org.neo4j.perftest.enterprise.util.Configuration.settingsOf;
import static org.neo4j.perftest.enterprise.util.Predicate.integerRange;
import static org.neo4j.perftest.enterprise.util.Setting.adaptSetting;
import static org.neo4j.perftest.enterprise.util.Setting.booleanSetting;
import static org.neo4j.perftest.enterprise.util.Setting.enumSetting;
import static org.neo4j.perftest.enterprise.util.Setting.integerSetting;
import static org.neo4j.perftest.enterprise.util.Setting.listSetting;
import static org.neo4j.perftest.enterprise.util.Setting.restrictSetting;
import static org.neo4j.perftest.enterprise.util.Setting.stringSetting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.ProgressIndicator;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.perftest.enterprise.util.Configuration;
import org.neo4j.perftest.enterprise.util.Conversion;
import org.neo4j.perftest.enterprise.util.Parameters;
import org.neo4j.perftest.enterprise.util.Setting;

public class DataGenerator
{
    public static final Random RANDOM = new Random();

    enum PropertyGenerator
    {
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
                        int length = 4 + RANDOM.nextInt( 60 );
                        StringBuilder result = new StringBuilder( length );
                        for ( int i = 0; i < length; i++ )
                        {
                            result.append( (char) ('a' + RANDOM.nextInt( 'z' - 'a' )) );
                        }
                        return result.toString();
                    }
                };

        abstract Object generate();
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

    static final Setting<String> store_dir = stringSetting( "neo4j.store_dir" );
    static final Setting<Boolean> report_progress = booleanSetting( "report_progress", false );
    static final Setting<Integer> node_count = adaptSetting(
            restrictSetting( integerSetting( "node_count" ), integerRange( 0, Integer.MAX_VALUE ) ),
            Conversion.TO_INTEGER );
    static final Setting<List<RelationshipSpec>> relationships = listSetting(
            adaptSetting( stringSetting( "relationships" ), RelationshipSpec.FROM_STRING ) );
    static final Setting<List<PropertyGenerator>> node_properties = listSetting(
            enumSetting( PropertyGenerator.class, "node_properties" ) );
    static final Setting<List<PropertyGenerator>> relationship_properties = listSetting(
            enumSetting( PropertyGenerator.class, "relationship_properties" ) );

    private final boolean reportProgress;
    private final int nodeCount;
    private final int relationshipCount;
    private final List<RelationshipSpec> relationshipsForEachNode;
    private final List<PropertyGenerator> nodeProperties;
    private final List<PropertyGenerator> relationshipProperties;

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
        ProgressIndicator progress = initProgress();
        generateNodes( batchInserter, progress );
        generateRelationships( batchInserter, progress );
    }

    private void generateNodes( BatchInserter batchInserter, ProgressIndicator progress )
    {
        for ( int i = 1 /*reference node already exists*/; i < nodeCount; i++ )
        {
            batchInserter.createNode( generate( nodeProperties ) );
            progress.update( false, i );
        }
        progress.done( nodeCount );
    }

    private void generateRelationships( BatchInserter batchInserter, ProgressIndicator progress )
    {
        for ( int i = 0; i < nodeCount; i++ )
        {
            for ( RelationshipSpec relationshipSpec : relationshipsForEachNode )
            {
                for ( int j = 0; j < relationshipSpec.count; j++ )
                {
                    batchInserter.createRelationship( i, RANDOM.nextInt( nodeCount ), relationshipSpec,
                                                      generate( relationshipProperties ) );
                    progress.update( true, 1 );
                }
            }
        }
        progress.done( relationshipCount );
    }

    protected ProgressIndicator initProgress()
    {
        if ( reportProgress )
        {
            System.out.println( "Generating " + this );
            return ProgressIndicator.MultiProgress.textual( System.out, nodeCount + relationshipCount );
        }
        else
        {
            return ProgressIndicator.NONE;
        }
    }

    private Map<String, Object> generate( List<PropertyGenerator> properties )
    {
        Map<String, Object> result = new HashMap<String, Object>();
        int id = 0;
        for ( PropertyGenerator property : properties )
        {
            result.put( property.name() + "_" + (id++), property.generate() );
        }
        return result;
    }

    public static void main( String... args )
    {
        run( Parameters.configuration( SYSTEM_PROPERTIES, settingsOf( DataGenerator.class ) ).convert( args ) );
    }

    static void run( Configuration configuration )
    {
        DataGenerator generator = new DataGenerator( configuration );
        BatchInserter batchInserter = new BatchInserterImpl( configuration.get( store_dir ),
                                                             generator.batchInserterConfig() );
        try
        {
            generator.generateData( batchInserter );
        }
        finally
        {
            batchInserter.shutdown();
        }
    }

    private Map<String, String> batchInserterConfig()
    {
        Map<String, String> config = new HashMap<String, String>();
        config.put( "use_memory_mapped_buffers", "true" );
        config.put( "dump_configuration", "true" );
        config.put( "neostore.nodestore.db.mapped_memory", "1200M" );
        config.put( "neostore.relationshipstore.db.mapped_memory", "5000M" );
        config.put( "neostore.propertystore.db.mapped_memory", "2000M" );
        return config;
    }
}
