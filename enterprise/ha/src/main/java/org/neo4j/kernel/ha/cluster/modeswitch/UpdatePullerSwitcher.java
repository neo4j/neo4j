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

import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.MasterUpdatePuller;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.ha.UpdatePuller;

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

    public UpdatePullerSwitcher( DelegateInvocationHandler<UpdatePuller> delegate, PullerFactory pullerFactory )
    {
        super( delegate );
        this.pullerFactory = pullerFactory;
    }

    @Override
    protected UpdatePuller getMasterImpl()
    {
        return MasterUpdatePuller.INSTANCE;
    }

    @Override
    protected UpdatePuller getSlaveImpl()
    {
        return pullerFactory.createSlaveUpdatePuller();
    }

    @Override
    protected void shutdownOldDelegate( UpdatePuller updatePuller )
    {
        if ( updatePuller != null )
        {
            updatePuller.stop();
        }
    }

    @Override
    protected void startNewDelegate( UpdatePuller updatePuller )
    {
        if ( updatePuller != null )
        {
            updatePuller.start();
        }
    }

}
