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
package org.neo4j.unsafe.impl.batchimport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.neo4j.helpers.collection.Iterators;

import static java.lang.Integer.max;

/**
 * Keeps data about how relationships are distributed between different types.
 */
public class RelationshipTypeDistribution implements Iterable<RelationshipTypeDistribution.RelationshipTypeCount>
{
    private final List<Client> clients = new ArrayList<>();
    private int opened;
    private RelationshipTypeCount[] typeCounts;

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
        System.out.println( "newClient " + opened );
        return client;
    }

    private synchronized void closeClient()
    {
        if ( --opened == 0 )
        {
            System.out.println( "closeClient yeah" );
            int highestTypeId = 0;
            for ( Client client : clients )
            {
                highestTypeId = max( highestTypeId, client.highestTypeId );
            }

            long[] counts = new long[highestTypeId];
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
        else
        {
            System.out.println( "closeClient now at " + opened );
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
    }

    public class Client implements AutoCloseable
    {
        private long[] counts = new long[8]; // index is relationship type id
        private int highestTypeId;

        public void increment( int typeId )
        {
            if ( typeId >= counts.length )
            {
                counts = Arrays.copyOf( counts, max( counts.length * 2, typeId ) );
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
            for ( int i = 0; i < highestTypeId; i++ )
            {
                counts[i] += this.counts[i];
            }
        }
    }
}
