/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.test;

import java.util.concurrent.CountDownLatch;

public interface Barrier
{
    Barrier NONE = new Barrier()
    {
        @Override
        public void reached()
        {
        }
    };

    void reached();

    class Control implements Barrier
    {
        private final CountDownLatch reached = new CountDownLatch( 1 ), released = new CountDownLatch( 1 );

        public void reached()
        {
            try
            {
                reached.countDown();
                released.await();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }

        public void await() throws InterruptedException
        {
            reached.await();
        }

        public void release()
        {
            released.countDown();
        }
    }
}
