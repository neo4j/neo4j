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
package org.neo4j.cypher.internal.codegen;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.LongStream;

import org.neo4j.graphdb.Node;

import static java.lang.String.format;

public class PrimitiveNodeStream extends PrimitiveEntityStream<NodeIdWrapper>
{
    public PrimitiveNodeStream( LongStream inner )
    {
        super( inner );
    }

    public static PrimitiveNodeStream of( long[] array )
    {
        return new PrimitiveNodeStream( LongStream.of( array ) );
    }

    public static PrimitiveNodeStream of( Object list )
    {
        if ( list == null )
        {
            return empty;
        }
        if ( list instanceof List )
        {
            return new PrimitiveNodeStream( ((List<Node>) list).stream().mapToLong( Node::getId ) );
        }
        else if ( list instanceof Node[] )
        {
            return new PrimitiveNodeStream( Arrays.stream( (Node[]) list ).mapToLong( Node::getId ) );
        }
        throw new IllegalArgumentException( format( "Can not convert to stream: %s", list.getClass().getName() ) );
    }

    @Override
    // This method is only used when we do not know the element type at compile time, so it has to box the elements
    public Iterator<NodeIdWrapper> iterator()
    {
        return inner.mapToObj( NodeIdWrapper::new ).iterator();
    }

    private static final PrimitiveNodeStream empty = new PrimitiveNodeStream( LongStream.empty() );
}
