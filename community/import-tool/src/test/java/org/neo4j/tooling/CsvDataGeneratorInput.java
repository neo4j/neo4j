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
package org.neo4j.tooling;

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.function.Function;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Deserialization;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputNodeDeserialization;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputRelationshipDeserialization;

/**
 * Uses {@link CsvDataGenerator} as an {@link Input} directly into a {@link BatchImporter}.
 */
public class CsvDataGeneratorInput extends CsvDataGenerator<InputNode,InputRelationship> implements Input
{
    private final IdType idType;
    private final Collector badCollector;

    public CsvDataGeneratorInput( final Header nodeHeader, final Header relationshipHeader,
            Configuration config, long nodes, long relationships, final Groups groups, final IdType idType,
            int numberOfLabels, int numberOfRelationshipTypes, Collector badCollector )
    {
        super( nodeHeader, relationshipHeader, config, nodes, relationships,
                new Function<SourceTraceability,Deserialization<InputNode>>()
                {
                    @Override
                    public Deserialization<InputNode> apply( SourceTraceability source ) throws RuntimeException
                    {
                        return new InputNodeDeserialization( source, nodeHeader, groups, idType.idsAreExternal() );
                    }
                },
                new Function<SourceTraceability,Deserialization<InputRelationship>>()
                {
                    @Override
                    public Deserialization<InputRelationship> apply( SourceTraceability from ) throws RuntimeException
                    {
                        return new InputRelationshipDeserialization( from, relationshipHeader, groups );
                    }
                },
                numberOfLabels, numberOfRelationshipTypes );
        this.idType = idType;
        this.badCollector = badCollector;
    }

    @Override
    public InputIterable<InputNode> nodes()
    {
        return new InputIterable<InputNode>()
        {
            @Override
            public InputIterator<InputNode> iterator()
            {
                return nodeData();
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
                return relationshipData();
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
