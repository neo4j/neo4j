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
package org.neo4j.kernel.guard;

import org.neo4j.logging.Log;

import static java.lang.System.currentTimeMillis;

public class Guard
{
    private final ThreadLocal<GuardInternal> threadLocal = new ThreadLocal<>();

    private final Log log;

    public Guard( final Log log )
    {
        this.log = log;
    }

    public void check()
    {
        GuardInternal guardInternal = currentGuard();
        if ( guardInternal != null )
        {
            guardInternal.check();
        }
    }

    public <T extends GuardInternal> T currentGuard()
    {
        return (T) threadLocal.get();
    }

    public void startOperationsCount( final long maxOps )
    {
        start( new OperationsCount( maxOps ) );
    }

    public void startTimeout( final long validForInMilliSeconds )
    {
        final Timeout timeout = new Timeout( validForInMilliSeconds + currentTimeMillis() );
        start( timeout );
    }

    public void start( final GuardInternal guard )
    {
        threadLocal.set( guard );
    }

    public <T extends GuardInternal> T stop()
    {
        T guardInternal = Guard.this.<T>currentGuard();
        if ( guardInternal != null )
        {
            threadLocal.remove();
        }
        return guardInternal;
    }

    public interface GuardInternal
    {
        void check();
    }

    public class OperationsCount implements GuardInternal
    {
        private final long max;
        private long opsCount = 0;

        private OperationsCount( final long max )
        {
            this.max = max;
        }

        @Override
        public void check()
        {
            opsCount++;

            if ( max < opsCount )
            {
                log.warn( "guard-timeout: node-ops: more than " + max );
                throw new GuardOperationsCountException( opsCount );
            }
        }

        public long getOpsCount()
        {
            return opsCount;
        }
    }

    public class Timeout implements GuardInternal
    {
        private final long valid;
        private final long start;

        private Timeout( final long valid )
        {
            this.valid = valid;
            this.start = currentTimeMillis();
        }

        @Override
        public void check()
        {
            if ( valid < currentTimeMillis() )
            {
                final long overtime = currentTimeMillis() - valid;
                log.warn( "guard-timeout:" + (valid - start) + "(+" + overtime + ")ms" );
                throw new GuardTimeoutException( overtime );
            }
        }
    }
}
