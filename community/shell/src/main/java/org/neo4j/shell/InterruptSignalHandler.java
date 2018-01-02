/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell;

import java.util.concurrent.atomic.AtomicReference;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import org.neo4j.helpers.Cancelable;

public class InterruptSignalHandler implements SignalHandler, CtrlCHandler
{
    private static InterruptSignalHandler INSTANCE = new InterruptSignalHandler();
    private final Signal signal = new Signal( "INT" );
    private final AtomicReference<Runnable> actionRef = new AtomicReference<>();

    private InterruptSignalHandler()
    {
    }

    public static InterruptSignalHandler getHandler()
    {
        return INSTANCE;
    }

    @Override
    public Cancelable install( final Runnable action )
    {
        if ( !actionRef.compareAndSet( null, action ) )
        {
            throw new RuntimeException( "An action has already been registered" );
        }

        final SignalHandler oldHandler = Signal.handle( signal, this );
        final InterruptSignalHandler self = this;
        return new Cancelable()
        {
            @Override
            public void cancel()
            {
                SignalHandler handle = Signal.handle( signal, oldHandler );
                if ( self != handle )
                {
                    throw new RuntimeException( "Error uninstalling ShellSignalHandler: " +
                            "another handler interjected in the mean time" );
                }
                if ( !self.actionRef.compareAndSet( action, null ) )
                {
                    throw new RuntimeException( "Popping a action that has not been pushed before" );
                }
            }
        };
    }

    public void handle( Signal signal )
    {
        try
        {
            actionRef.get().run();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
