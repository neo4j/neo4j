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
package org.neo4j.kernel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

/**
 * Handle the collection of kernel event handlers, and fire events as needed.
 *
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class KernelEventHandlers
    implements Lifecycle
{
    private final List<KernelEventHandler> kernelEventHandlers = new CopyOnWriteArrayList<>();
    private final Log log;

    public KernelEventHandlers( Log log )
    {
        this.log = log;
    }

    @Override
    public void init()
        throws Throwable
    {
    }

    @Override
    public void start()
        throws Throwable
    {
    }

    @Override
    public void stop()
        throws Throwable
    {
    }

    @Override
    public void shutdown()
        throws Throwable
    {
        for( KernelEventHandler kernelEventHandler : kernelEventHandlers )
        {
            kernelEventHandler.beforeShutdown();
        }
    }

    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        if( this.kernelEventHandlers.contains( handler ) )
        {
            return handler;
        }

        // Some algo for putting it in the right place
        for( KernelEventHandler registeredHandler : this.kernelEventHandlers )
        {
            KernelEventHandler.ExecutionOrder order =
                handler.orderComparedTo( registeredHandler );
            int index = this.kernelEventHandlers.indexOf( registeredHandler );
            if( order == KernelEventHandler.ExecutionOrder.BEFORE )
            {
                this.kernelEventHandlers.add( index, handler );
                return handler;
            }
            else if( order == KernelEventHandler.ExecutionOrder.AFTER )
            {
                this.kernelEventHandlers.add( index + 1, handler );
                return handler;
            }
        }

        this.kernelEventHandlers.add( handler );
        return handler;
    }

    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        if( !kernelEventHandlers.remove( handler ) )
        {
            throw new IllegalStateException( handler + " isn't registered" );
        }
        return handler;
    }

    public void kernelPanic( ErrorState error, Throwable cause )
    {
        for( KernelEventHandler handler : kernelEventHandlers )
        {
            try
            {
                handler.kernelPanic( error );
            }
            catch( Throwable e )
            {
                if ( cause != null )
                {
                    e.addSuppressed( cause );
                }
                log.error( "FATAL: Error while handling kernel panic.", e );
            }
        }
    }
}
