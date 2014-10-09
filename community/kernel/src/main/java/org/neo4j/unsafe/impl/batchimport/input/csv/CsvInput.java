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

import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapping;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.CharSeeker;

/**
 * Provides {@link Input} from data contained in tabular/csv form. Expects factories for instantiating
 * the {@link CharSeeker} objects seeking values in the csv data and header factories for how to
 * extract meta data about the values.
 */
public class CsvInput implements Input
{
    private final DataFactory nodeDataFactory;
    private final Header.Factory nodeHeaderFactory;
    private final DataFactory relationshipDataFactory;
    private final Header.Factory relationshipHeaderFactory;
    private final IdType idType;
    private final IdMapping idMapping;
    private final Configuration config;

    private final int[] delimiter;

    public CsvInput(
            DataFactory nodeDataFactory, Header.Factory nodeHeaderFactory,
            DataFactory relationshipDataFactory, Header.Factory relationshipHeaderFactory,
            IdType idType, Configuration config )
    {
        this.nodeDataFactory = nodeDataFactory;
        this.nodeHeaderFactory = nodeHeaderFactory;
        this.relationshipDataFactory = relationshipDataFactory;
        this.relationshipHeaderFactory = relationshipHeaderFactory;
        this.idType = idType;
        this.idMapping = idType.idMapping();
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
                // Open the data stream. It's closed by the batch importer when execution is done.
                final CharSeeker dataStream = nodeDataFactory.create( config );

                // Read the header, given the data stream. This allows the header factory to be able to
                // parse the header from the data stream directly. Or it can decide to grab the header
                // from somewhere else, it's up to that factory.
                final Header dataHeader = nodeHeaderFactory.create( dataStream, config, idType.extractor() );

                return new InputNodeDeserializer( dataHeader, dataStream, delimiter );
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
                // Open the data stream. It's closed by the batch importer when execution is done.
                final CharSeeker dataStream = relationshipDataFactory.create( config );

                // Read the header, given the data stream. This allows the header factory to be able to
                // parse the header from the data stream directly. Or it can decide to grab the header
                // from somewhere else, it's up to that factory.
                final Header dataHeader = relationshipHeaderFactory.create( dataStream, config, idType.extractor() );

                return new InputRelationshipDeserializer( dataHeader, dataStream, delimiter );
            }
        };
    }

    @Override
    public IdMapping idMapping()
    {
        return idMapping;
    }
}
