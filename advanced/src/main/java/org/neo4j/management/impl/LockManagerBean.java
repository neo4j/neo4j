/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.management.LockManager;

@Service.Implementation( ManagementBeanProvider.class )
public final class LockManagerBean extends ManagementBeanProvider
{
    public LockManagerBean()
    {
        super( LockManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new LockManagerImpl( this, kernel );
    }

    @Description( "Information about the Neo4j lock status" )
    private static class LockManagerImpl extends Neo4jMBean implements LockManager
    {
        private final org.neo4j.kernel.impl.transaction.LockManager lockManager;

        LockManagerImpl( ManagementBeanProvider provider, KernelData kernel )
                throws NotCompliantMBeanException
        {
            super( provider, kernel );
            this.lockManager = kernel.getConfig().getLockManager();
        }

        @Description( "The number of lock sequences that would have lead to a deadlock situation that "
                      + "Neo4j has detected and adverted (by throwing DeadlockDetectedException)." )
        public long getNumberOfAdvertedDeadlocks()
        {
            return lockManager.getDetectedDeadlockCount();
        }
    }
}
