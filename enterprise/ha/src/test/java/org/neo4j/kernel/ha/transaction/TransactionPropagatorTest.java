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
package org.neo4j.kernel.ha.transaction;

import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.cluster.InstanceId;
import org.neo4j.com.Response;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlavePriorities;
import org.neo4j.kernel.ha.com.master.SlavePriority;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.ha.transaction.TransactionPropagator.Configuration;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.Log;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.ha.HaSettings.TxPushStrategy.fixed_ascending;
import static org.neo4j.kernel.ha.HaSettings.TxPushStrategy.fixed_descending;
import static org.neo4j.kernel.ha.HaSettings.tx_push_strategy;

public class TransactionPropagatorTest
{
    @Rule
    public final LifeRule life = new LifeRule( true );

    @Test
    public void shouldCapUndesiredSlaveCountPushLogging()
    {
        // GIVEN
        int serverId = 1;
        final InstanceId instanceId = new InstanceId( serverId );
        Configuration config = new Configuration()
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
                return SlavePriorities.fixedDescending();
            }
        };
        Log logger = mock( Log.class );
        Slaves slaves = mock( Slaves.class );
        when( slaves.getSlaves() ).thenReturn( Collections.emptyList() );
        CommitPusher pusher = mock( CommitPusher.class );
        TransactionPropagator propagator = life.add( new TransactionPropagator( config, logger, slaves, pusher ) );

        // WHEN
        for ( int i = 0; i < 10; i++ )
        {
            propagator.committed( TransactionIdStore.BASE_TX_ID, serverId );
        }

        // THEN
        verify( logger, times( 1 ) ).info( anyString() );
    }

    @Test
    public void shouldPrioritizeAscendingIfAsked()
    {
        // GIVEN
        Configuration propagator = TransactionPropagator
                .from( Config.defaults( tx_push_strategy, fixed_ascending.name() ) );
        SlavePriority strategy = propagator.getReplicationStrategy();

        // WHEN
        Iterable<Slave> prioritize = strategy.prioritize( asList( slave( 1 ), slave( 0 ), slave( 2 ) ) );

        // THEN
        assertThat( Iterables.asList( prioritize ), equalTo( asList( slave( 0 ), slave( 1 ), slave( 2 ) ) ) );
    }

    @Test
    public void shouldPrioritizeDescendingIfAsked()
    {
        // GIVEN
        Configuration propagator = TransactionPropagator
                .from( Config.defaults( tx_push_strategy, fixed_descending.name() ) );
        SlavePriority strategy = propagator.getReplicationStrategy();

        // WHEN
        Iterable<Slave> prioritize = strategy.prioritize( asList( slave( 1 ), slave( 0 ), slave( 2 ) ) );

        // THEN
        assertThat( Iterables.asList(prioritize), equalTo( asList( slave( 2 ), slave( 1 ), slave( 0 ) ) ) );
    }

    private Slave slave( final int id )
    {
        return new Slave()
        {
            @Override
            public Response<Void> pullUpdates( long upToAndIncludingTxId )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getServerId()
            {
                return id;
            }

            @Override
            public boolean equals( Object obj )
            {
                return obj instanceof Slave && ((Slave) obj).getServerId() == id;
            }

            @Override
            public int hashCode()
            {
                return id;
            }

            @Override
            public String toString()
            {
                return "Slave[" + id + "]";
            }
        };
    }
}
