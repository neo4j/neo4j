/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import java.time.Clock;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.LockingCompatibilityTestSuite;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.test.OtherThreadExecutor.WaitDetails;

public class ForsetiLocksTest extends LockingCompatibilityTestSuite
{
    @Override
    protected Locks createLockManager( Config config, Clock clock )
    {
        return new ForsetiLockManager( config, clock, ResourceTypes.values() );
    }

    @Override
    protected boolean isAwaitingLockAcquisition( WaitDetails details )
    {
        return details.isAt( ForsetiClient.class, "applyWaitStrategy" );
    }
}
