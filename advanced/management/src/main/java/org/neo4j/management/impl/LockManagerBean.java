/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.management.impl;

import java.util.ArrayList;
import java.util.List;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.management.LockManager;

@Service.Implementation( ManagementBeanProvider.class )
public final class LockManagerBean extends ManagementBeanProvider
{
    public LockManagerBean()
    {
        super( LockManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new LockManagerImpl( management );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new LockManagerImpl( management, true );
    }

    private static class LockManagerImpl extends Neo4jMBean implements LockManager
    {
        private final Locks lockManager;

        LockManagerImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.lockManager = lockManager( management );
        }

        private Locks lockManager( ManagementData management )
        {
            try
            {
                return management.getKernelData().graphDatabase().getDependencyResolver()
                        .resolveDependency( Locks.class );
            }
            catch(Throwable e)
            {
                e.printStackTrace();
                return null;
            }
        }

        LockManagerImpl( ManagementData management, boolean mxBean )
        {
            super( management, mxBean );
            this.lockManager = lockManager( management );
        }

        @Override
        public long getNumberOfAvertedDeadlocks()
        {
            return -1l;
        }

        @Override
        public List<LockInfo> getLocks()
        {
            final List<LockInfo> locks = new ArrayList<>();
            lockManager.accept( new Locks.Visitor()
            {
                @Override
                public void visit( Locks.ResourceType resourceType, long resourceId, String description, long waitTime,
                        long lockIdentityHashCode )
                {
                    locks.add( new LockInfo( resourceType.toString(), String.valueOf( resourceId ), description ) );
                }
            });
            return locks;
        }

        @Override
        public List<LockInfo> getContendedLocks( final long minWaitTime )
        {
            // Contended locks can no longer be found by the new lock manager, since that knowledge is not centralized.
            return getLocks();
        }
    }
}
