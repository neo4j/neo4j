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
package org.neo4j.kernel.ha.factory;

import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.CommitProcessSwitcher;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.ModeSwitcherNotifier;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class HighlyAvailableCommitProcessFactoryTest
{
    @Test
    public void createReadOnlyCommitProcess()
    {
        HighlyAvailableCommitProcessFactory factory = new HighlyAvailableCommitProcessFactory(
                mock( LifeSupport.class ), mock( Master.class ), mock( TransactionPropagator.class ),
                mock( RequestContextFactory.class ), mock( HighAvailabilityModeSwitcher.class ) );

        Config config = new Config( stringMap( GraphDatabaseSettings.read_only.name(), "true" ) );

        TransactionCommitProcess commitProcess = factory.create( mock( TransactionAppender.class ),
                mock( TransactionRepresentationStoreApplier.class ), mock( NeoStoreInjectedTransactionValidator.class ),
                mock( IndexUpdatesValidator.class ), config );

        assertThat( commitProcess, instanceOf( ReadOnlyTransactionCommitProcess.class ) );
    }

    @Test
    public void createRegularCommitProcess()
    {
        LifeSupport modeSwitchersLife = mock( LifeSupport.class );
        when( modeSwitchersLife.getLifecycleInstances() ).thenReturn( Iterables.<Lifecycle>empty() );

        HighlyAvailableCommitProcessFactory factory = new HighlyAvailableCommitProcessFactory(
                modeSwitchersLife, mock( Master.class ), mock( TransactionPropagator.class ),
                mock( RequestContextFactory.class ), mock( HighAvailabilityModeSwitcher.class ) );

        TransactionCommitProcess commitProcess = factory.create( mock( TransactionAppender.class ),
                mock( TransactionRepresentationStoreApplier.class ), mock( NeoStoreInjectedTransactionValidator.class ),
                mock( IndexUpdatesValidator.class ), new Config() );

        assertThat( commitProcess, not( instanceOf( ReadOnlyTransactionCommitProcess.class ) ) );
        assertThat( Proxy.getInvocationHandler( commitProcess ), instanceOf( DelegateInvocationHandler.class ) );
    }

    @Test
    public void removesOldCommitProcessSwitcherFromLifeWhenNewOneIsCreated() throws Throwable
    {
        CommitProcessSwitcher oldCommitSwitcher1 = newCommitProcessSwitcher();
        CommitProcessSwitcher oldCommitSwitcher2 = newCommitProcessSwitcher();

        LifeSupport life = new LifeSupport();
        life.add( oldCommitSwitcher1 );
        life.add( oldCommitSwitcher2 );

        HighlyAvailableCommitProcessFactory factory = new HighlyAvailableCommitProcessFactory( life,
                mock( Master.class ), mock( TransactionPropagator.class ), mock( RequestContextFactory.class ),
                mock( HighAvailabilityModeSwitcher.class ) );

        TransactionCommitProcess commitProcess = factory.create( mock( TransactionAppender.class ),
                mock( TransactionRepresentationStoreApplier.class ), mock( NeoStoreInjectedTransactionValidator.class ),
                mock( IndexUpdatesValidator.class ), new Config() );

        assertNotNull( commitProcess );

        List<Lifecycle> lifecycle = Iterables.toList( life.getLifecycleInstances() );
        assertEquals( 1, lifecycle.size() );

        assertFalse( lifecycle.contains( oldCommitSwitcher1 ) );
        assertFalse( lifecycle.contains( oldCommitSwitcher2 ) );
    }

    @SuppressWarnings( "unchecked" )
    private CommitProcessSwitcher newCommitProcessSwitcher()
    {
        return new CommitProcessSwitcher( mock( TransactionPropagator.class ), mock( Master.class ),
                mock( DelegateInvocationHandler.class ), mock( RequestContextFactory.class ),
                mock( ModeSwitcherNotifier.class ), mock( NeoStoreInjectedTransactionValidator.class ),
                mock( TransactionCommitProcess.class ) );
    }
}
