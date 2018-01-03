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
package org.neo4j.cluster.statemachine;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * Handle for dynamic proxies that are backed by a {@link StateMachine}.
 * Delegates calls to a {@link StateMachineProxyFactory}, which in turn
 * will call the {@link StateMachine}.
 */
public class StateMachineProxyHandler
        implements InvocationHandler
{
    private StateMachineProxyFactory stateMachineProxyFactory;
    private StateMachine stateMachine;

    public StateMachineProxyHandler( StateMachineProxyFactory stateMachineProxyFactory, StateMachine stateMachine )
    {
        this.stateMachineProxyFactory = stateMachineProxyFactory;
        this.stateMachine = stateMachine;
    }

    @Override
    public Object invoke( Object proxy, Method method, Object[] args )
            throws Throwable
    {
        // Delegate call to factory, which will translate method call into state machine invocation
        return stateMachineProxyFactory.invoke( stateMachine, method, args == null ? null : (args.length > 1 ? args :
                args[0]) );
    }

    public StateMachineProxyFactory getStateMachineProxyFactory()
    {
        return stateMachineProxyFactory;
    }
}
