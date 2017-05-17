/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint;

import org.neo4j.unsafe.impl.batchimport.CountBasedStates;
import org.neo4j.unsafe.impl.batchimport.GeneratingInputIterator;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Collectors;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import static java.lang.Math.toIntExact;

public class NodeCountInputs implements Input
{
    private static final Object[] properties = new Object[]{
            "a", 10, "b", 10, "c", 10, "d", 10, "e", 10, "f", 10, "g", 10, "h", 10, "i", 10,
            "j", 10, "k", 10, "l", 10, "m", 10, "o", 10, "p", 10, "q", 10, "r", 10, "s", 10
    };
    private static final String[] labels = new String[]{"a", "b", "c", "d"};

    private final long nodeCount;
    private final Collector bad = Collectors.silentBadCollector( 0 );

    public NodeCountInputs( long nodeCount )
    {
        this.nodeCount = nodeCount;
    }

    @Override
    public InputIterator nodes()
    {
        return new GeneratingInputIterator<Void>( new CountBasedStates<Void>( nodeCount, 1_000 )
        {
            @Override
            protected Void nextState()
            {
                return null;
            }
        } )
        {
            @Override
            protected boolean generateNext( Void state, long batch, int itemInBatch, InputEntityVisitor visitor )
            {
                int item = toIntExact( batch * 1_000 + itemInBatch );
                if ( item > nodeCount )
                {
                    return false;
                }

                visitor.id( item, Group.GLOBAL );
                visitor.labels( labels );
                for ( int i = 0; i < properties.length; i++ )
                {
                    visitor.property( (String) properties[i++], properties[i] );
                }
                return true;
            }
        };
    }

    @Override
    public InputIterator relationships()
    {
        return GeneratingInputIterator.EMPTY;
    }

    @Override
    public IdMapper idMapper()
    {
        return IdMappers.actual();
    }

    @Override
    public Collector badCollector()
    {
        return bad;
    }
}
