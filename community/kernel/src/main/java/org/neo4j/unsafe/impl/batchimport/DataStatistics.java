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
package org.neo4j.unsafe.impl.batchimport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.helpers.collection.Iterators;

import static java.lang.Integer.max;
import static java.lang.String.format;

/**
 * Keeps data about how relationships are distributed between different types.
 */
public class DataStatistics implements Iterable<DataStatistics.RelationshipTypeCount>
{
    private final List<Client> clients = new ArrayList<>();
    private int opened;
    private RelationshipTypeCount[] typeCounts;
    private final long nodeCount;
    private final long propertyCount;

    public DataStatistics( long nodeCount, long propertyCount, RelationshipTypeCount[] sortedTypes )
    {
        this.nodeCount = nodeCount;
        this.propertyCount = propertyCount;
        this.typeCounts = sortedTypes;
    }

    @Override
    public Iterator<RelationshipTypeCount> iterator()
    {
        return Iterators.iterator( typeCounts );
    }

    public int getNumberOfRelationshipTypes()
    {
        return typeCounts.length;
    }

    public synchronized Client newClient()
    {
        Client client = new Client();
        clients.add( client );
        opened++;
        return client;
    }

    private synchronized void closeClient()
    {
        if ( --opened == 0 )
        {
            int highestTypeId = 0;
            for ( Client client : clients )
            {
                highestTypeId = max( highestTypeId, client.highestTypeId );
            }

            long[] counts = new long[highestTypeId + 1];
            for ( Client client : clients )
            {
                client.addTo( counts );
            }
            typeCounts = new RelationshipTypeCount[counts.length];
            for ( int i = 0; i < counts.length; i++ )
            {
                typeCounts[i] = new RelationshipTypeCount( i, counts[i] );
            }
            Arrays.sort( typeCounts );
        }
    }

    public static class RelationshipTypeCount implements Comparable<RelationshipTypeCount>
    {
        private final int typeId;
        private final long count;

        public RelationshipTypeCount( int typeId, long count )
        {
            this.typeId = typeId;
            this.count = count;
        }

        public int getTypeId()
        {
            return typeId;
        }

        public long getCount()
        {
            return count;
        }

        @Override
        public int compareTo( RelationshipTypeCount o )
        {
            return Long.compare( count, o.count );
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (count ^ (count >>> 32));
            result = prime * result + typeId;
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj == null || getClass() != obj.getClass() )
            {
                return false;
            }
            RelationshipTypeCount other = (RelationshipTypeCount) obj;
            return count == other.count && typeId == other.typeId;
        }

        @Override
        public String toString()
        {
            return format( "%s[type:%d, count:%d]", getClass().getSimpleName(), typeId, count );
        }
    }

    public class Client implements AutoCloseable
    {
        private long[] counts = new long[8]; // index is relationship type id
        private int highestTypeId;

        public void increment( int typeId )
        {
            if ( typeId >= counts.length )
            {
                counts = Arrays.copyOf( counts, max( counts.length * 2, typeId + 1 ) );
            }
            counts[typeId]++;
            if ( typeId > highestTypeId )
            {
                highestTypeId = typeId;
            }
        }

        @Override
        public void close()
        {
            closeClient();
        }

        private void addTo( long[] counts )
        {
            for ( int i = 0; i <= highestTypeId; i++ )
            {
                counts[i] += this.counts[i];
            }
        }
    }

    public RelationshipTypeCount get( int index )
    {
        return typeCounts[index];
    }

    public PrimitiveIntSet types( int startingFromType, int upToType )
    {
        PrimitiveIntSet set = Primitive.intSet( (upToType - startingFromType) * 2 );
        for ( int i = startingFromType; i < upToType; i++ )
        {
            set.add( get( i ).getTypeId() );
        }
        return set;
    }

    public long getNodeCount()
    {
        return nodeCount;
    }

    public long getPropertyCount()
    {
        return propertyCount;
    }

    public long getRelationshipCount()
    {
        long sum = 0;
        for ( RelationshipTypeCount type : typeCounts )
        {
            sum += type.count;
        }
        return sum;
    }

    @Override
    public String toString()
    {
        return format( "Imported:%n  %d nodes%n  %d relationships%n  %d properties", nodeCount, getRelationshipCount(), propertyCount );
    }
}
