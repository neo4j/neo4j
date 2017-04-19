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
package org.neo4j.collection.pool;

interface LinkedQueuePoolMonitor<R>
{
    void updatedCurrentPeakSize( int currentPeakSize );

    void updatedTargetSize( int targetSize );

    void created( R resource );

    void acquired( R resource );

    void disposed( R resource );

    class Adapter<R> implements LinkedQueuePoolMonitor<R>
    {
        @Override
        public void updatedCurrentPeakSize( int currentPeakSize )
        {
        }

        @Override
        public void updatedTargetSize( int targetSize )
        {
        }

        @Override
        public void created( R resource )
        {
        }

        @Override
        public void acquired( R resource )
        {
        }

        @Override
        public void disposed( R resource )
        {
        }
    }
}
