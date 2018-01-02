/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.function.Function;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.MissingRelationshipDataException;

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
    private final Groups groups = new Groups();
    private final Collector badCollector;

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
            IdType idType, Configuration config, Collector badCollector )
    {
        assertSaneConfiguration( config );

        this.nodeDataFactory = nodeDataFactory;
        this.nodeHeaderFactory = nodeHeaderFactory;
        this.relationshipDataFactory = relationshipDataFactory;
        this.relationshipHeaderFactory = relationshipHeaderFactory;
        this.idType = idType;
        this.config = config;
        this.badCollector = badCollector;
    }

    private void assertSaneConfiguration( Configuration config )
    {
        Map<Character,String> delimiters = new HashMap<>();
        delimiters.put( config.delimiter(), "delimiter" );
        checkUniqueCharacter( delimiters, config.arrayDelimiter(), "array delimiter" );
        checkUniqueCharacter( delimiters, config.quotationCharacter(), "quotation character" );
    }

    private void checkUniqueCharacter( Map<Character,String> characters, char character, String characterDescription )
    {
        String conflict = characters.put( character, characterDescription );
        if ( conflict != null )
        {
            throw new IllegalArgumentException( "Character '" + character + "' specified by " + characterDescription +
                    " is the same as specified by " + conflict );
        }
    }

    @Override
    public InputIterable<InputNode> nodes()
    {
        return new InputIterable<InputNode>()
        {
            @Override
            public InputIterator<InputNode> iterator()
            {
                return new InputGroupsDeserializer<InputNode>( nodeDataFactory.iterator(),
                        nodeHeaderFactory, config, idType )
                {
                    @Override
                    protected InputEntityDeserializer<InputNode> entityDeserializer( CharSeeker dataStream,
                            Header dataHeader, Function<InputNode,InputNode> decorator )
                    {
                        return new InputEntityDeserializer<>( dataHeader, dataStream, config.delimiter(),
                                new InputNodeDeserialization( dataStream, dataHeader, groups, idType.idsAreExternal() ),
                                decorator, Validators.<InputNode>emptyValidator(), badCollector );
                    }
                };
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    @Override
    public InputIterable<InputRelationship> relationships()
    {
        return new InputIterable<InputRelationship>()
        {
            @Override
            public InputIterator<InputRelationship> iterator()
            {
                return new InputGroupsDeserializer<InputRelationship>( relationshipDataFactory.iterator(),
                        relationshipHeaderFactory, config, idType )
                {
                    @Override
                    protected InputEntityDeserializer<InputRelationship> entityDeserializer( CharSeeker dataStream,
                              Header dataHeader, Function<InputRelationship,InputRelationship> decorator )
                    {
                        return new InputEntityDeserializer<>( dataHeader, dataStream, config.delimiter(),
                                new InputRelationshipDeserialization( dataStream, dataHeader, groups ),
                                decorator, new Validator<InputRelationship>()
                                {
                                    @Override
                                    public void validate( InputRelationship entity )
                                    {
                                        if ( entity.startNode() == null )
                                        {
                                            throw new MissingRelationshipDataException(Type.START_ID,
                                                                entity + " is missing " + Type.START_ID + " field" );
                                        }
                                        if ( entity.endNode() == null )
                                        {
                                            throw new MissingRelationshipDataException(Type.END_ID,
                                                                entity + " is missing " + Type.END_ID + " field" );
                                        }
                                        if ( !entity.hasTypeId() && entity.type() == null )
                                        {
                                            throw new MissingRelationshipDataException(Type.TYPE,
                                                                entity + " is missing " + Type.TYPE + " field" );
                                        }
                                    }
                                }, badCollector );
                    }
                };
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
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

    @Override
    public boolean specificRelationshipIds()
    {
        return false;
    }

    @Override
    public Collector badCollector()
    {
        return badCollector;
    }
}
