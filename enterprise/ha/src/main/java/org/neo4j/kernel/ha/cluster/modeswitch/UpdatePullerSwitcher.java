/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.lifecycle.LifeSupport;

/**
 * UpdatePullerSwitcher will provide different implementations of {@link UpdatePuller}
 * depending on node mode (slave or master).
 *
 * @see UpdatePuller
 * @see SlaveUpdatePuller
 */
public class UpdatePullerSwitcher extends AbstractComponentSwitcher<UpdatePuller>
{
    private final PullerFactory pullerFactory;

    // Field is volatile because it is used by threads from executor in HighAvailabilityModeSwitcher
    private volatile LifeSupport life;

    public UpdatePullerSwitcher( DelegateInvocationHandler<UpdatePuller> delegate, PullerFactory pullerFactory )
    {
        super( delegate );
        this.pullerFactory = pullerFactory;
    }

    @Override
    protected UpdatePuller getMasterImpl()
    {
        return UpdatePuller.NONE;
    }

    @Override
    protected UpdatePuller getSlaveImpl()
    {
        SlaveUpdatePuller slaveUpdatePuller = pullerFactory.createSlaveUpdatePuller();
        startUpdatePuller( slaveUpdatePuller );
        return slaveUpdatePuller;
    }

    @Override
    protected void shutdownCurrent()
    {
        super.shutdownCurrent();
        shutdownCurrentPuller();
    }

    private void shutdownCurrentPuller()
    {
        if ( life != null )
        {
            life.shutdown();
            life = null;
        }
    }

    private void startUpdatePuller( SlaveUpdatePuller slaveUpdatePuller )
    {
        life = new LifeSupport();
        life.add( slaveUpdatePuller );
        life.start();
    }
}
