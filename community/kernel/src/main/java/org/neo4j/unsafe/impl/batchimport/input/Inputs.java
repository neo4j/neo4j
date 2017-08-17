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
package org.neo4j.unsafe.impl.batchimport.input;

import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;

public class Inputs
{
    private Inputs()
    {
    }

    public static Input input(
            final InputIterable<InputNode> nodes, final InputIterable<InputRelationship> relationships,
            final IdMapper idMapper, final IdGenerator idGenerator, final Collector badCollector )
    {
        return new Input()
        {
            @Override
            public InputIterable<InputRelationship> relationships()
            {
                return relationships;
            }

            @Override
            public InputIterable<InputNode> nodes()
            {
                return nodes;
            }

            @Override
            public IdMapper idMapper( NumberArrayFactory numberArrayFactory )
            {
                return idMapper;
            }

            @Override
            public IdGenerator idGenerator()
            {
                return idGenerator;
            }

            @Override
            public Collector badCollector()
            {
                return badCollector;
            }
        };
    }
}
