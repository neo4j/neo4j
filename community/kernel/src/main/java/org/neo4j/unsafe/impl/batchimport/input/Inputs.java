/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.File;

import org.neo4j.function.Functions;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactory;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;

import static org.neo4j.helpers.collection.Iterables.asResourceIterable;
import static org.neo4j.helpers.collection.Iterables.iterable;

public class Inputs
{
    public static Input input( final Iterable<InputNode> nodes, final Iterable<InputRelationship> relationships,
            final IdMapper idMapper, final IdGenerator idGenerator )
    {
        final ResourceIterable<InputNode> resourceNodes = asResourceIterable( nodes );
        final ResourceIterable<InputRelationship> resourceRelationships = asResourceIterable( relationships );
        return new Input()
        {
            @Override
            public ResourceIterable<InputRelationship> relationships()
            {
                return resourceRelationships;
            }

            @Override
            public ResourceIterable<InputNode> nodes()
            {
                return resourceNodes;
            }

            @Override
            public IdMapper idMapper()
            {
                return idMapper;
            }

            @Override
            public IdGenerator idGenerator()
            {
                return idGenerator;
            }
        };
    }

    public static Input csv( File nodes, File relationships, IdType idType,
            Configuration configuration )
    {
        Iterable<DataFactory<InputNode>> nodeData =
                iterable( DataFactories.data( Functions.<InputNode>identity(), nodes ) );
        Iterable<DataFactory<InputRelationship>> relationshipData =
                iterable( DataFactories.data( Functions.<InputRelationship>identity(), relationships ) );
        return new CsvInput(
                nodeData, DataFactories.defaultFormatNodeFileHeader(),
                relationshipData, DataFactories.defaultFormatRelationshipFileHeader(),
                idType, configuration );
    }
}
