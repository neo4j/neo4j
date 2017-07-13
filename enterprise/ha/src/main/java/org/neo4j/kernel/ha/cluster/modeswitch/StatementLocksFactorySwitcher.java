/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster.modeswitch;

import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.lock.SlaveStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;

/**
 * Statement locks factory switcher that will use original configured locks factory in case if
 * instance is master, other wise slave specific locks factory will be provided that have additional
 * capabilities of acquiring some shared locks on master during commit
 */
public class StatementLocksFactorySwitcher extends AbstractComponentSwitcher<StatementLocksFactory>
{
    private final StatementLocksFactory configuredStatementLocksFactory;

    public StatementLocksFactorySwitcher( DelegateInvocationHandler<StatementLocksFactory> delegate,
            StatementLocksFactory configuredStatementLocksFactory )
    {
        super( delegate );
        this.configuredStatementLocksFactory = configuredStatementLocksFactory;
    }

    @Override
    protected StatementLocksFactory getMasterImpl()
    {
        return configuredStatementLocksFactory;
    }

    @Override
    protected StatementLocksFactory getSlaveImpl()
    {
        return new SlaveStatementLocksFactory( configuredStatementLocksFactory );
    }
}
