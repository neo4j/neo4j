/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.function.Function;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.store.BatchingIdSequence;

/**
 * Provides {@link Input} from data contained in tabular/csv form. Expects factories for instantiating
 * the {@link CharSeeker} objects seeking values in the csv data and header factories for how to
 * extract meta data about the values.
 */
public class CsvInput implements Input
{
    private final Iterable<DataFactory<InputNode>> nodeDataFactory;
    private final Header.Factory nodeHeaderFactory;
    private final Iterable<DataFactory<InputRelationship>> relationshipDataFactory;
    private final Header.Factory relationshipHeaderFactory;
    private final IdType idType;
    private final Configuration config;
    private final int[] delimiter;
    private final BatchingIdSequence relationshipIds = new BatchingIdSequence();

    /**
     * @param nodeDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code nodeHeaderFactory}. From the outside
     * it looks like one stream of nodes.
     * @param nodeHeaderFactory factory for reading node headers.
     * @param relationshipDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code relationshipHeaderFactory}.
     * From the outside it looks like one stream of relationships.
     * @param relationshipHeaderFactory factory for reading relationship headers.
     * @param idType {@link IdType} to expect in id fields of node and relationship input.
     * @param config CSV configuration.
     */
    public CsvInput(
            Iterable<DataFactory<InputNode>> nodeDataFactory, Header.Factory nodeHeaderFactory,
            Iterable<DataFactory<InputRelationship>> relationshipDataFactory, Header.Factory relationshipHeaderFactory,
            IdType idType, Configuration config )
    {
        this.nodeDataFactory = nodeDataFactory;
        this.nodeHeaderFactory = nodeHeaderFactory;
        this.relationshipDataFactory = relationshipDataFactory;
        this.relationshipHeaderFactory = relationshipHeaderFactory;
        this.idType = idType;
        this.config = config;

        this.delimiter = new int[] {config.delimiter()};
    }

    @Override
    public ResourceIterable<InputNode> nodes()
    {
        return new ResourceIterable<InputNode>()
        {
            @Override
            public ResourceIterator<InputNode> iterator()
            {
                return new InputGroupsDeserializer<InputNode>( nodeDataFactory.iterator(),
                                                               nodeHeaderFactory, config, idType )
                {
                    @Override
                    protected ResourceIterator<InputNode> entityDeserializer( CharSeeker dataStream, Header dataHeader,
                                                                              Function<InputNode,InputNode> decorator )
                    {
                        return new InputNodeDeserializer( dataHeader, dataStream, delimiter, decorator,
                                idType.idsAreExternal() );
                    }
                };
            }
        };
    }

    @Override
    public ResourceIterable<InputRelationship> relationships()
    {
        return new ResourceIterable<InputRelationship>()
        {
            @Override
            public ResourceIterator<InputRelationship> iterator()
            {
                relationshipIds.reset();
                return new InputGroupsDeserializer<InputRelationship>( relationshipDataFactory.iterator(),
                                                                       relationshipHeaderFactory, config, idType )
                {
                    @Override
                    protected ResourceIterator<InputRelationship> entityDeserializer( CharSeeker dataStream,
                              Header dataHeader, Function<InputRelationship,InputRelationship> decorator )
                    {
                        return new InputRelationshipDeserializer( dataHeader, dataStream, delimiter,
                                relationshipIds, decorator );
                    }
                };
            }
        };
    }

    @Override
    public IdMapper idMapper()
    {
        return idType.idMapper();
    }

    @Override
    public IdGenerator idGenerator()
    {
        return idType.idGenerator();
    }
}
