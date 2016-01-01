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
package org.neo4j.kernel.ha.transaction;

import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.cluster.InstanceId;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlavePriorities;
import org.neo4j.kernel.ha.com.master.SlavePriority;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.logging.LogMarker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransactionPropagatorTest
{
    @Rule
    public final LifeRule life = new LifeRule( true );

    @Test
    public void shouldCapUndesiredSlaveCountPushLogging() throws Exception
    {
        // GIVEN
        int serverId = 1;
        final InstanceId instanceId = new InstanceId( serverId );
        TransactionPropagator.Configuration config = new TransactionPropagator.Configuration()
        {
            @Override
            public int getTxPushFactor()
            {
                return 1;
            }

            @Override
            public InstanceId getServerId()
            {
                return instanceId;
            }

            @Override
            public SlavePriority getReplicationStrategy()
            {
                return SlavePriorities.fixed();
            }
        };
        StringLogger logger = mock( StringLogger.class );
        Slaves slaves = mock( Slaves.class );
        when( slaves.getSlaves() ).thenReturn( Collections.<Slave>emptyList() );
        CommitPusher pusher = mock( CommitPusher.class );
        TransactionPropagator propagator = life.add( new TransactionPropagator( config, logger, slaves, pusher ) );

        // WHEN
        for ( int i = 0; i < 10; i++ )
        {
            propagator.committed( TransactionIdStore.BASE_TX_ID, serverId );
        }

        // THEN
        verify( logger, times( 1 ) ).info( anyString(), any( Throwable.class ), anyBoolean(), any( LogMarker.class ) );
    }
}
