/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.register;

import org.neo4j.io.pagecache.impl.muninn.jsr166e.StampedLock;

public class ConcurrentRegisters
{
    public static class OptimisticRead
    {
        public static Register.DoubleLongRegister newDoubleLongRegister()
        {
            return newDoubleLongRegister( -1l, -1l );
        }

        public static Register.DoubleLongRegister newDoubleLongRegister( final long initialFirst,
                                                                         final long initialSecond )
        {
            return new Register.DoubleLongRegister()
            {
                private long first = initialFirst;
                private long second = initialSecond;

                private final StampedLock lock = new StampedLock();

                @Override
                public long readFirst()
                {
                    long stamp = lock.tryOptimisticRead();
                    long result = first;
                    if ( !lock.validate( stamp ) )
                    {
                        stamp = lock.readLock();
                        try
                        {
                            result = first;
                        }
                        finally
                        {
                            lock.unlock( stamp );
                        }
                    }
                    return result;
                }

                @Override
                public long readSecond()
                {
                    long stamp = lock.tryOptimisticRead();
                    long result = second;
                    if ( !lock.validate( stamp ) )
                    {
                        stamp = lock.readLock();
                        try
                        {
                            result = second;
                        }
                        finally
                        {
                            lock.unlock( stamp );
                        }
                    }
                    return result;
                }

                @Override
                public void copyTo( Register.DoubleLong.Out target )
                {
                    long stamp = lock.tryOptimisticRead();
                    long firstCopy = this.first;
                    long secondCopy = this.second;
                    if ( !lock.validate( stamp ) )
                    {
                        stamp = lock.readLock();
                        try
                        {
                            firstCopy = this.first;
                            secondCopy = this.second;
                        }
                        finally
                        {
                            lock.unlock( stamp );
                        }
                    }
                    target.write( firstCopy, secondCopy );
                }

                @Override
                public void write( long first, long second )
                {
                    long stamp = lock.writeLock();
                    try
                    {
                        this.first = first;
                        this.second = second;
                    }
                    finally
                    {
                        lock.unlock( stamp );
                    }
                }

                @Override
                public void increment( long firstDelta, long secondDelta )
                {
                    long stamp = lock.writeLock();
                    try
                    {
                        this.first += firstDelta;
                        this.second += secondDelta;
                    }
                    finally
                    {
                        lock.unlock( stamp );
                    }
                }
            };
        }
    }
}
