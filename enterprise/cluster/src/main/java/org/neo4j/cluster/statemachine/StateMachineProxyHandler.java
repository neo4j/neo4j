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
    private StateMachine<?,?> stateMachine;

    public StateMachineProxyHandler( StateMachineProxyFactory stateMachineProxyFactory, StateMachine<?,?> stateMachine )
    {
        this.stateMachineProxyFactory = stateMachineProxyFactory;
        this.stateMachine = stateMachine;
    }

    @Override
    public Object invoke( Object proxy, Method method, Object[] args )
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
