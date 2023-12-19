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
package org.neo4j.kernel.ha.cluster.modeswitch;

import org.junit.Test;

import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.lock.SlaveStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class StatementLocksFactorySwitcherTest
{

    private StatementLocksFactory configuredLockFactory = mock( StatementLocksFactory.class );

    @Test
    public void masterStatementLocks()
    {
        StatementLocksFactorySwitcher switcher = getLocksSwitcher();
        StatementLocksFactory masterLocks = switcher.getMasterImpl();
        assertSame( masterLocks, configuredLockFactory );
    }

    @Test
    public void slaveStatementLocks()
    {
        StatementLocksFactorySwitcher switcher = getLocksSwitcher();
        StatementLocksFactory slaveLocks = switcher.getSlaveImpl();
        assertThat( slaveLocks, instanceOf( SlaveStatementLocksFactory.class ) );
    }

    private StatementLocksFactorySwitcher getLocksSwitcher()
    {
        DelegateInvocationHandler invocationHandler = mock( DelegateInvocationHandler.class );
        return new StatementLocksFactorySwitcher( invocationHandler, configuredLockFactory );
    }
}
