/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.util;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import org.neo4j.graphdb.Direction;
import org.neo4j.storageengine.api.Degrees;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;

public class EagerDegrees implements Degrees, Degrees.Mutator
{
    private static final int FIRST_TYPE_UNDECIDED = -1;

    private int firstType = FIRST_TYPE_UNDECIDED;
    private Degree firstTypeDegrees;
    private MutableIntObjectMap<Degree> degrees;

    @Override
    public boolean add( int type, int outgoing, int incoming, int loop )
    {
        getOrCreateDegree( type ).add( outgoing, incoming, loop );
        return true;
    }

    public void addOutgoing( int type, int count )
    {
        getOrCreateDegree( type ).outgoing += count;
    }

    public void addIncoming( int type, int count )
    {
        getOrCreateDegree( type ).incoming += count;
    }

    public void addLoop( int type, int count )
    {
        getOrCreateDegree( type ).loop += count;
    }

    private Degree getOrCreateDegree( int type )
    {
        if ( firstType == FIRST_TYPE_UNDECIDED )
        {
            firstType = type;
            firstTypeDegrees = new Degree();
            return firstTypeDegrees;
        }
        else if ( firstType == type )
        {
            return firstTypeDegrees;
        }

        if ( degrees == null )
        {
            degrees = IntObjectMaps.mutable.empty();
        }
        return degrees.getIfAbsentPut( type, Degree::new );
    }

    @Override
    public int[] types()
    {
        if ( firstType == FIRST_TYPE_UNDECIDED )
        {
            return EMPTY_INT_ARRAY;
        }
        if ( degrees == null )
        {
            return new int[]{firstType};
        }
        int[] types = new int[degrees.size() + 1];
        types[0] = firstType;
        System.arraycopy( degrees.keySet().toArray(), 0, types, 1, degrees.size() );
        return types;
    }

    @Override
    public int degree( int type, Direction direction )
    {
        Degree degree = null;
        if ( firstType == type )
        {
            degree = firstTypeDegrees;
        }
        else if ( degrees != null )
        {
            degree = degrees.get( type );
        }
        if ( degree == null )
        {
            return 0;
        }
        switch ( direction )
        {
        case OUTGOING:
            return degree.outgoing + degree.loop;
        case INCOMING:
            return degree.incoming + degree.loop;
        case BOTH:
            return degree.outgoing + degree.incoming + degree.loop;
        default:
            throw new IllegalArgumentException( "Unrecognized direction " + direction );
        }
    }

    @Override
    public boolean isSplit()
    {
        return true;
    }

    private static class Degree
    {
        private int outgoing;
        private int incoming;
        private int loop;

        void add( int outgoing, int incoming, int loop )
        {
            this.outgoing += outgoing;
            this.incoming += incoming;
            this.loop += loop;
        }
    }
}
