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

import org.junit.Test;

import org.neo4j.kernel.ha.DelegateInvocationHandler;

import static org.junit.Assert.assertEquals;

public class AbstractComponentSwitcherTest
{
    @Test
    public void switchesToMaster() throws Throwable
    {
        DelegateInvocationHandler<Component> delegate = new DelegateInvocationHandler<>( Component.class );
        TestComponentSwitcher switcher = new TestComponentSwitcher( delegate );

        switcher.switchToMaster();

        assertEquals( delegateClass( delegate ), MasterComponent.class );
    }

    @Test
    public void switchesToSlave() throws Throwable
    {
        DelegateInvocationHandler<Component> delegate = new DelegateInvocationHandler<>( Component.class );
        TestComponentSwitcher switcher = new TestComponentSwitcher( delegate );

        switcher.switchToSlave();

        assertEquals( delegateClass( delegate ), SlaveComponent.class );
    }

    @Test
    public void switchesToPending() throws Throwable
    {
        DelegateInvocationHandler<Component> delegate = new DelegateInvocationHandler<>( Component.class );
        TestComponentSwitcher switcher = new TestComponentSwitcher( delegate );

        switcher.switchToPending();

        assertEquals( delegateClass( delegate ), PendingComponent.class );
    }

    private static Class<?> delegateClass( DelegateInvocationHandler<?> invocationHandler ) throws Throwable
    {
        return (Class<?>) invocationHandler.invoke( new Object(), Object.class.getMethod( "getClass" ), new Object[0] );
    }

    private static class TestComponentSwitcher extends AbstractComponentSwitcher<Component>
    {
        TestComponentSwitcher( DelegateInvocationHandler<Component> delegate )
        {
            super( delegate );
        }

        @Override
        protected Component getMasterImpl()
        {
            return new MasterComponent();
        }

        @Override
        protected Component getSlaveImpl()
        {
            return new SlaveComponent();
        }

        @Override
        protected Component getPendingImpl()
        {
            return new PendingComponent();
        }
    }

    private interface Component
    {
    }

    private static class MasterComponent implements Component
    {
    }

    private static class SlaveComponent implements Component
    {
    }

    private static class PendingComponent implements Component
    {
    }
}
