/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.net.URI;

import org.neo4j.kernel.ha.cluster.AbstractModeSwitcher;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.logging.Logging;

public class LabelTokenCreatorModeSwitcher extends AbstractModeSwitcher<TokenCreator>
{
    private final HaXaDataSourceManager xaDsm;
    private final DelegateInvocationHandler<Master> master;
    private final RequestContextFactory requestContextFactory;
    private final Logging logging;

    public LabelTokenCreatorModeSwitcher( HighAvailabilityMemberStateMachine stateMachine,
                                          DelegateInvocationHandler<TokenCreator> delegate,
                                          HaXaDataSourceManager xaDsm,
                                          DelegateInvocationHandler<Master> master,
                                          RequestContextFactory requestContextFactory, Logging logging
    )
    {
        super( stateMachine, delegate );
        this.xaDsm = xaDsm;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.logging = logging;
    }

    @Override
    protected TokenCreator getMasterImpl()
    {
        return new DefaultLabelIdCreator( logging );
    }

    @Override
    protected TokenCreator getSlaveImpl( URI serverHaUri )
    {
        return new SlaveLabelTokenCreator( master.cement(), requestContextFactory, xaDsm );
    }
}
