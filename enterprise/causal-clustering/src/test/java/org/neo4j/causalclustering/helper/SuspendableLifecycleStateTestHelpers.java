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
package org.neo4j.causalclustering.helper;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.lifecycle.Lifecycle;

class SuspendableLifecycleStateTestHelpers
{
    static void setInitialState( StateAwareSuspendableLifeCycle lifeCycle, LifeCycleState state ) throws Throwable
    {
        for ( LifeCycleState lifeCycleState : LifeCycleState.values() )
        {
            if ( lifeCycleState.compareTo( state ) <= 0 )
            {
                lifeCycleState.set( lifeCycle );
            }
        }
    }

    enum LifeCycleState
    {
        Init( Lifecycle::init ),
        Start( Lifecycle::start ),
        Stop( Lifecycle::stop ),
        Shutdown( Lifecycle::shutdown );

        private final ThrowingConsumer<Lifecycle,Throwable> operation;

        LifeCycleState( ThrowingConsumer<Lifecycle,Throwable> operation )
        {
            this.operation = operation;
        }

        void set( Lifecycle lifecycle ) throws Throwable
        {
            operation.accept( lifecycle );
        }
    }

    enum SuspendedState
    {
        Untouched( suspendable -> {} ),
        Enabled( Suspendable::enable ),
        Disabled( Suspendable::disable );

        private final ThrowingConsumer<Suspendable,Throwable> consumer;

        SuspendedState( ThrowingConsumer<Suspendable,Throwable> consumer )
        {
            this.consumer = consumer;
        }

        void set( Suspendable suspendable ) throws Throwable
        {
            consumer.accept( suspendable );
        }
    }
}
