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

import java.util.function.ToIntFunction;

import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Input.Estimates;
import org.neo4j.values.storable.Value;

public class Inputs
{
    private Inputs()
    {
    }

    public static Input input(
            final InputIterable nodes, final InputIterable relationships,
            final IdMapper idMapper, final Collector badCollector, Estimates estimates )
    {
        return new Input()
        {
            @Override
            public InputIterable relationships()
            {
                return relationships;
            }

            @Override
            public InputIterable nodes()
            {
                return nodes;
            }

            @Override
            public IdMapper idMapper( NumberArrayFactory numberArrayFactory )
            {
                return idMapper;
            }

            @Override
            public Collector badCollector()
            {
                return badCollector;
            }

            @Override
            public Estimates calculateEstimates( ToIntFunction<Value[]> valueSizeCalculator )
            {
                return estimates;
            }
        };
    }

    public static Estimates knownEstimates(
            long numberOfNodes, long numberOfRelationships,
            long numberOfNodeProperties, long numberOfRelationshipProperties,
            long nodePropertiesSize, long relationshipPropertiesSize,
            long numberOfNodeLabels )
    {
        return new Estimates()
        {
            @Override
            public long numberOfNodes()
            {
                return numberOfNodes;
            }

            @Override
            public long numberOfRelationships()
            {
                return numberOfRelationships;
            }

            @Override
            public long numberOfNodeProperties()
            {
                return numberOfNodeProperties;
            }

            @Override
            public long sizeOfNodeProperties()
            {
                return nodePropertiesSize;
            }

            @Override
            public long numberOfNodeLabels()
            {
                return numberOfNodeLabels;
            }

            @Override
            public long numberOfRelationshipProperties()
            {
                return numberOfRelationshipProperties;
            }

            @Override
            public long sizeOfRelationshipProperties()
            {
                return relationshipPropertiesSize;
            }
        };
    }

    public static int calculatePropertySize( InputEntity entity, ToIntFunction<Value[]> valueSizeCalculator )
    {
        int size = 0;
        int propertyCount = entity.propertyCount();
        if ( propertyCount > 0 )
        {
            Value[] values = new Value[propertyCount];
            for ( int i = 0; i < propertyCount; i++ )
            {
                values[i] = ValueUtils.asValue( entity.propertyValue( i ) );
            }
            size += valueSizeCalculator.applyAsInt( values );
        }
        return size;
    }
}
