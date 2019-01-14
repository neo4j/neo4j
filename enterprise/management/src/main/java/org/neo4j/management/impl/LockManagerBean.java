/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
    protected Neo4jMBean createMXBean( ManagementData management )
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
            catch ( Throwable e )
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
            return -1L;
        }

        @Override
        public List<LockInfo> getLocks()
        {
            final List<LockInfo> locks = new ArrayList<>();
            lockManager.accept( ( resourceType, resourceId, description, waitTime, lockIdentityHashCode ) ->
                    locks.add( new LockInfo( resourceType.toString(), String.valueOf( resourceId ), description ) ) );
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
