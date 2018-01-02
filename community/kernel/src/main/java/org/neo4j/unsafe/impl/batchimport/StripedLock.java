/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.graphdb.Resource;

/**
 * An array of locks which can be {@link #lock(int) locked} in a try-with-resource block, and also unlocked
 * there of course.
 */
public class StripedLock
{
    private final Stripe[] stripes;

    public StripedLock( int stripes )
    {
        this.stripes = new Stripe[stripes];
        for ( int i = 0; i < stripes; i++ )
        {
            this.stripes[i] = new Stripe();
        }
    }

    public Resource lock( int stripe )
    {
        Stripe lock = stripes[stripe];
        lock.lock.lock();
        return lock;
    }

    private static class Stripe implements Resource
    {
        private final Lock lock = new ReentrantLock();

        @Override
        public void close()
        {
            lock.unlock();
        }
    }
}
