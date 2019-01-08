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
