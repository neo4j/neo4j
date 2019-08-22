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
package org.neo4j.cypher.internal.profiling;

import org.neo4j.internal.kernel.api.KernelReadTracer;

public abstract class OperatorProfileEvent implements AutoCloseable, KernelReadTracer
{
    public abstract void dbHit();

    public abstract void dbHits( int hits );

    public abstract void row();

    public abstract void rows( int n );

    // AutoCloseable

    @Override
    public abstract void close();

    // KernelReadTracer

    @Override
    public void onNode( long nodeReference )
    {
        dbHit();
    }

    @Override
    public void onAllNodesScan()
    {
        dbHit();
    }

    @Override
    public void onLabelScan( int label )
    {
        dbHit();
    }

    @Override
    public void onRelationship( long relationshipReference )
    {
        dbHit();
    }

    @Override
    public void onRelationshipGroup( int type )
    {
        dbHit();
    }

    @Override
    public void onProperty( int propertyKey )
    {
        dbHit();
    }

    @Override
    public void onIndexSeek()
    {
        dbHit();
    }

    public static final OperatorProfileEvent NONE = new OperatorProfileEvent()
    {
        @Override
        public void dbHit()
        {
        }

        @Override
        public void dbHits( int hits )
        {

        }

        @Override
        public void row()
        {
        }

        @Override
        public void rows( int n )
        {
        }

        @Override
        public void close()
        {
        }
    };
}
