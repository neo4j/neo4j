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
package org.neo4j.kernel.ha;

import org.neo4j.kernel.ha.cluster.AbstractModeSwitcher;
import org.neo4j.kernel.ha.cluster.ModeSwitcherNotifier;
import org.neo4j.kernel.lifecycle.LifeSupport;

/**
 * UpdatePullerModeSwitcher will provide different implementations of {@link UpdatePuller}
 * depending on node mode (slave or master).
 *
 * @see UpdatePuller
 * @see SlaveUpdatePuller
 */
public class UpdatePullerModeSwitcher extends AbstractModeSwitcher<UpdatePuller>
{

    private final PullerFactory pullerFactory;

    public UpdatePullerModeSwitcher( ModeSwitcherNotifier notifier,
            DelegateInvocationHandler<UpdatePuller> delegate, PullerFactory pullerFactory )
    {
        super( notifier, delegate );
        this.pullerFactory = pullerFactory;
    }

    @Override
    protected UpdatePuller getSlaveImpl( LifeSupport life )
    {
        return pullerFactory.createUpdatePuller( life );
    }

    @Override
    protected UpdatePuller getMasterImpl( LifeSupport life )
    {
        return UpdatePuller.NONE;
    }
}
