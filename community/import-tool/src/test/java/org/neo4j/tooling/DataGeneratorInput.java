/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.tooling;

import org.neo4j.csv.reader.Extractors;
import org.neo4j.unsafe.impl.batchimport.IdRangeInput.Range;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;

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

    public DataGeneratorInput( long nodes, long relationships, IdType idType, Collector badCollector, long seed,
            Header nodeHeader, Header relationshipHeader, int labelCount, int relationshipTypeCount )
    {
        this.nodes = nodes;
        this.relationships = relationships;
        this.idType = idType;
        this.badCollector = badCollector;
        this.seed = seed;
        this.nodeHeader = nodeHeader;
        this.relationshipHeader = relationshipHeader;
        this.labels = new Distribution<>( tokens( "Label", labelCount ) );
        this.relationshipTypes = new Distribution<>( tokens( "TYPE", relationshipTypeCount ) );
    }

    @Override
    public InputIterator nodes()
    {
        return new RandomEntityDataGenerator( nodes, nodes, 10_000, seed, nodeHeader, labels, relationshipTypes );
    }

    @Override
    public InputIterator relationships()
    {
        return new RandomEntityDataGenerator( nodes, relationships, 10_000, seed, relationshipHeader,
                labels, relationshipTypes );
    }

    @Override
    public IdMapper idMapper()
    {
        return idType.idMapper();
    }

    @Override
    public Collector badCollector()
    {
        return badCollector;
    }

    public static Header sillyNodeHeader( IdType idType, Extractors extractors )
    {
        return new Header( new Entry[] {
                new Entry( null, Type.ID, null, idType.extractor( extractors ) ),
                new Entry( "name", Type.PROPERTY, null, extractors.string() ),
                new Entry( "age", Type.PROPERTY, null, extractors.int_() ),
                new Entry( "something", Type.PROPERTY, null, extractors.string() ),
                new Entry( null, Type.LABEL, null, extractors.stringArray() ),
        } );
    }

    public static Header bareboneNodeHeader( IdType idType, Extractors extractors )
    {
        return new Header( new Entry[] {
                new Entry( null, Type.ID, null, idType.extractor( extractors ) ),
                new Entry( null, Type.LABEL, null, extractors.stringArray() ),
        } );
    }

    public static Header bareboneRelationshipHeader( IdType idType, Extractors extractors )
    {
        return new Header( new Entry[] {
                new Entry( null, Type.START_ID, null, idType.extractor( extractors ) ),
                new Entry( null, Type.END_ID, null, idType.extractor( extractors ) ),
                new Entry( null, Type.TYPE, null, extractors.string() )
        } );
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
