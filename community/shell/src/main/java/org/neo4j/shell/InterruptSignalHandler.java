/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.Closeable;
import java.io.IOException;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class InterruptSignalHandler implements SignalHandler, CtrlCHandler
{
    private static InterruptSignalHandler INSTANCE = new InterruptSignalHandler();
    private final Signal signal = new Signal( "INT" );
    private volatile Runnable action;

    private InterruptSignalHandler()
    {
    }

    public static InterruptSignalHandler getHandler()
    {
        return INSTANCE;
    }

    @Override
    public Closeable install( final Runnable action )
    {
        if ( this.action != null )
        {
            throw new RuntimeException( "An action has already been registered" );
        }
        this.action = action;
        final SignalHandler oldHandler = Signal.handle( signal, this );
        final InterruptSignalHandler self = this;
        return new Closeable()
        {
            @Override
            public void close() throws IOException
            {
                SignalHandler handle = Signal.handle( signal, oldHandler );
                if ( self != handle )
                {
                    throw new RuntimeException( "Error uninstalling ShellSignalHandler: another handler interjected in the " +
                            "mean time" );
                }
                if ( self.action != action )
                {
                    throw new RuntimeException( "Popping a action that has not been pushed before" );
                }
                self.action = null;
            }
        };
    }

    public void handle( Signal signal )
    {
        try
        {
            this.action.run();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }
}
