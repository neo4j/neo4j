/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;

import org.neo4j.csv.reader.Extractors;
import org.neo4j.unsafe.impl.batchimport.IdRangeInput.Range;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;
import org.neo4j.values.storable.Value;

import static java.util.Arrays.asList;

import static org.neo4j.unsafe.impl.batchimport.InputIterable.replayable;

/**
 * {@link Input} which generates data on the fly. This input wants to know number of nodes and relationships
 * and then a function for generating the nodes and another for generating the relationships.
 * Data can be generated in parallel and so those generator functions accepts a {@link Range} for which
 * an array of input objects are generated, everything else will be taken care of. So typical usage would be:
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
    private final Collector badCollector;
    private final long seed;
    private final Header nodeHeader;
    private final Header relationshipHeader;
    private final Distribution<String> labels;
    private final Distribution<String> relationshipTypes;
    private final float factorBadNodeData;
    private final float factorBadRelationshipData;
    private final long startId;
    private final Groups groups = new Groups();

    public DataGeneratorInput( long nodes, long relationships, IdType idType, Collector badCollector, long seed, long startId,
            Header nodeHeader, Header relationshipHeader, int labelCount, int relationshipTypeCount,
            float factorBadNodeData, float factorBadRelationshipData )
    {
        this.nodes = nodes;
        this.relationships = relationships;
        this.idType = idType;
        this.badCollector = badCollector;
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
    public InputIterable nodes()
    {
        return replayable( () -> new RandomEntityDataGenerator( nodes, nodes, 10_000, seed, startId, nodeHeader, labels, relationshipTypes,
                factorBadNodeData, factorBadRelationshipData ) );
    }

    @Override
    public InputIterable relationships()
    {
        return replayable( () -> new RandomEntityDataGenerator( nodes, relationships, 10_000, seed, startId, relationshipHeader,
                labels, relationshipTypes, factorBadNodeData, factorBadRelationshipData ) );
    }

    @Override
    public IdMapper idMapper( NumberArrayFactory numberArrayFactory )
    {
        return idType.idMapper( numberArrayFactory, groups );
    }

    @Override
    public Collector badCollector()
    {
        return badCollector;
    }

    @Override
    public Estimates calculateEstimates( ToIntFunction<Value[]> valueSizeCalculator )
    {
        int sampleSize = 100;
        InputEntity[] nodeSample = sample( nodes(), sampleSize );
        double labelsPerNodeEstimate = sampleLabels( nodeSample );
        double[] nodePropertyEstimate = sampleProperties( nodeSample, valueSizeCalculator );
        double[] relationshipPropertyEstimate = sampleProperties( sample( relationships(), sampleSize ), valueSizeCalculator );
        return Inputs.knownEstimates(
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
            labels += node.labels().length;
        }
        return (double) labels / nodes.length;
    }

    private static double[] sampleProperties( InputEntity[] sample, ToIntFunction<Value[]> valueSizeCalculator )
    {
        int propertiesPerEntity = sample[0].propertyCount();
        long propertiesSize = 0;
        for ( InputEntity entity : sample )
        {
            propertiesSize += Inputs.calculatePropertySize( entity, valueSizeCalculator );
        }
        double propertySizePerEntity = (double) propertiesSize / sample.length;
        return new double[] {propertiesPerEntity, propertySizePerEntity};
    }

    public static Header sillyNodeHeader( IdType idType, Extractors extractors )
    {
        return new Header( new Entry( null, Type.ID, null, idType.extractor( extractors ) ),
                new Entry( "name", Type.PROPERTY, null, extractors.string() ),
                new Entry( "age", Type.PROPERTY, null, extractors.int_() ),
                new Entry( "something", Type.PROPERTY, null, extractors.string() ),
                new Entry( null, Type.LABEL, null, extractors.stringArray() ) );
    }

    public static Header bareboneNodeHeader( IdType idType, Extractors extractors )
    {
        return bareboneNodeHeader( null, idType, extractors );
    }

    public static Header bareboneNodeHeader( String idKey, IdType idType, Extractors extractors, Entry... additionalEntries )
    {
        List<Entry> entries = new ArrayList<>();
        entries.add( new Entry( idKey, Type.ID, null, idType.extractor( extractors ) ) );
        entries.add( new Entry( null, Type.LABEL, null, extractors.stringArray() ) );
        entries.addAll( asList( additionalEntries ) );
        return new Header( entries.toArray( new Entry[entries.size()] ) );
    }

    public static Header bareboneRelationshipHeader( IdType idType, Extractors extractors, Entry... additionalEntries )
    {
        List<Entry> entries = new ArrayList<>();
        entries.add( new Entry( null, Type.START_ID, null, idType.extractor( extractors ) ) );
        entries.add( new Entry( null, Type.END_ID, null, idType.extractor( extractors ) ) );
        entries.add( new Entry( null, Type.TYPE, null, extractors.string() ) );
        entries.addAll( asList( additionalEntries ) );
        return new Header( entries.toArray( new Entry[entries.size()] ) );
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
