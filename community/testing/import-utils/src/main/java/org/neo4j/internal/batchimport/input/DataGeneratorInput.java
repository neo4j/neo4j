/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport.input;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.csv.reader.Extractors;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.csv.Header;
import org.neo4j.internal.batchimport.input.csv.Header.Entry;
import org.neo4j.internal.batchimport.input.csv.Type;

import static java.util.Arrays.asList;
import static org.neo4j.internal.batchimport.input.csv.CsvInput.idExtractor;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

/**
 * {@link Input} which generates data on the fly. This input wants to know number of nodes and relationships
 * and then a function for generating the nodes and another for generating the relationships.
 * So typical usage would be:
 *
 * <pre>
 * {@code
 * BatchImporter importer = ...
 * Input input = new DataGeneratorInput( 10_000_000, 1_000_000_000,
 *      batch -> {
 *          InputNode[] nodes = new InputNode[batch.getSize()];
 *          for ( int i = 0; i < batch.getSize(); i++ ) {
 *              long id = batch.getStart() + i;
 *              nodes[i] = new InputNode( .... );
 *          }
 *          return nodes;
 *      },
 *      batch -> {
 *          InputRelationship[] relationships = new InputRelationship[batch.getSize()];
 *          ....
 *          return relationships;
 *      } );
 * }
 * </pre>
 */
public class DataGeneratorInput implements Input
{
    private final long nodes;
    private final long relationships;
    private final IdType idType;
    private final long seed;
    private final Header nodeHeader;
    private final Header relationshipHeader;
    private final Distribution<String> labels;
    private final Distribution<String> relationshipTypes;
    private final float factorBadNodeData;
    private final float factorBadRelationshipData;
    private final long startId;
    private final Groups groups = new Groups();
    private int maxStringLength = 20;

    public DataGeneratorInput( long nodes, long relationships, IdType idType, long seed, long startId,
            Header nodeHeader, Header relationshipHeader, int labelCount, int relationshipTypeCount,
            float factorBadNodeData, float factorBadRelationshipData )
    {
        this.nodes = nodes;
        this.relationships = relationships;
        this.idType = idType;
        this.seed = seed;
        this.startId = startId;
        this.nodeHeader = nodeHeader;
        this.relationshipHeader = relationshipHeader;
        this.factorBadNodeData = factorBadNodeData;
        this.factorBadRelationshipData = factorBadRelationshipData;
        this.labels = new Distribution<>( tokens( "Label", labelCount ) );
        this.relationshipTypes = new Distribution<>( tokens( "TYPE", relationshipTypeCount ) );
    }

    @Override
    public InputIterable nodes( Collector badCollector )
    {
        return () -> new RandomEntityDataGenerator( nodes, nodes, 10_000, seed, startId, nodeHeader, labels, relationshipTypes,
                factorBadNodeData, factorBadRelationshipData, maxStringLength );
    }

    @Override
    public InputIterable relationships( Collector badCollector )
    {
        return () -> new RandomEntityDataGenerator( nodes, relationships, 10_000, seed, startId, relationshipHeader,
                labels, relationshipTypes, factorBadNodeData, factorBadRelationshipData, maxStringLength );
    }

    @Override
    public IdType idType()
    {
        return idType;
    }

    @Override
    public ReadableGroups groups()
    {
        return groups;
    }

    @Override
    public Estimates calculateEstimates( PropertySizeCalculator valueSizeCalculator )
    {
        int sampleSize = 100;
        InputEntity[] nodeSample = sample( nodes( Collector.EMPTY ), sampleSize );
        double labelsPerNodeEstimate = sampleLabels( nodeSample );
        double[] nodePropertyEstimate = sampleProperties( nodeSample, valueSizeCalculator );
        double[] relationshipPropertyEstimate = sampleProperties( sample( relationships( Collector.EMPTY ), sampleSize ), valueSizeCalculator );
        return Input.knownEstimates(
                nodes, relationships,
                (long) (nodes * nodePropertyEstimate[0]), (long) (relationships * relationshipPropertyEstimate[0]),
                (long) (nodes * nodePropertyEstimate[1]), (long) (relationships * relationshipPropertyEstimate[1]),
                (long) (nodes * labelsPerNodeEstimate) );
    }

    private InputEntity[] sample( InputIterable source, int size )
    {
        try ( InputIterator iterator = source.iterator();
              InputChunk chunk = iterator.newChunk() )
        {
            InputEntity[] sample = new InputEntity[size];
            int cursor = 0;
            while ( cursor < size && iterator.next( chunk ) )
            {
                while ( cursor < size && chunk.next( sample[cursor++] = new InputEntity() ) )
                {
                    // just loop
                }
            }
            return sample;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static double sampleLabels( InputEntity[] nodes )
    {
        int labels = 0;
        for ( InputEntity node : nodes )
        {
            if ( node != null )
            {
                labels += node.labels().length;
            }
        }
        return (double) labels / nodes.length;
    }

    private static double[] sampleProperties( InputEntity[] sample, PropertySizeCalculator valueSizeCalculator )
    {
        int propertiesPerEntity = sample[0].propertyCount();
        long propertiesSize = 0;
        for ( InputEntity entity : sample )
        {
            if ( entity != null )
            {
                propertiesSize += Inputs.calculatePropertySize( entity, valueSizeCalculator, NULL, INSTANCE );
            }
        }
        double propertySizePerEntity = (double) propertiesSize / sample.length;
        return new double[] {propertiesPerEntity, propertySizePerEntity};
    }

    public static Header bareboneNodeHeader( IdType idType, Extractors extractors )
    {
        return bareboneNodeHeader( null, idType, extractors );
    }

    public static Header bareboneNodeHeader( String idKey, IdType idType, Extractors extractors, Entry... additionalEntries )
    {
        List<Entry> entries = new ArrayList<>();
        entries.add( new Entry( idKey, Type.ID, null, idExtractor( idType, extractors ) ) );
        entries.add( new Entry( null, Type.LABEL, null, extractors.stringArray() ) );
        entries.addAll( asList( additionalEntries ) );
        return new Header( entries.toArray( new Entry[0] ) );
    }

    public static Header bareboneRelationshipHeader( IdType idType, Extractors extractors, Entry... additionalEntries )
    {
        List<Entry> entries = new ArrayList<>();
        entries.add( new Entry( null, Type.START_ID, null, idExtractor( idType, extractors ) ) );
        entries.add( new Entry( null, Type.END_ID, null, idExtractor( idType, extractors ) ) );
        entries.add( new Entry( null, Type.TYPE, null, extractors.string() ) );
        entries.addAll( asList( additionalEntries ) );
        return new Header( entries.toArray( new Entry[0] ) );
    }

    private static String[] tokens( String prefix, int count )
    {
        String[] result = new String[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = prefix + (i + 1);
        }
        return result;
    }
}
