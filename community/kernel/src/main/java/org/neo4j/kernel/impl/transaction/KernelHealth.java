/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.helpers.Exceptions.withCause;

public class KernelHealth
{
    private static final String panicMessage = "Kernel has encountered some problem, "
            + "please perform neccesary action (tx recovery/restart)";

    // Keep that cozy name for legacy purposes
    private volatile boolean tmOk = true; // TODO rather skip volatile if possible here.
    private final KernelPanicEventGenerator kpe;
    private final StringLogger log;
    private Throwable causeOfPanic;

    public KernelHealth( KernelPanicEventGenerator kpe, Logging logging )
    {
        this.kpe = kpe;
        this.log = logging.getMessagesLog( getClass() );
    }

    public <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> panicDisguise )
            throws EXCEPTION
    {
        if ( !tmOk )
        {
            EXCEPTION exception = null;
            try
            {
                try
                {
                    exception = panicDisguise.getConstructor( String.class, Throwable.class )
                            .newInstance( panicMessage, causeOfPanic );
                }
                catch ( NoSuchMethodException e )
                {
                    exception = withCause( panicDisguise.getConstructor( String.class )
                            .newInstance( panicMessage ), causeOfPanic );
                }
            }
            catch ( Exception e )
            {
                throw new Error( panicMessage + ". An exception of type " + panicDisguise.getName() +
                        " was requested to be thrown but that proved imposslble", e );
            }
            throw exception;
        }
    }

    public void panic( Throwable cause )
    {
        if ( !tmOk )
        {
            return;
        }

        if ( cause == null )
        {
            throw new IllegalArgumentException( "Must provide a cause for the kernel panic" );
        }
        this.causeOfPanic = cause;
        this.tmOk = false;
        // Keeps the "setting TM not OK string for grep:ability
        log.error( "setting TM not OK. " + panicMessage, cause );
        kpe.generateEvent( ErrorState.TX_MANAGER_NOT_OK, causeOfPanic );
    }

    public void healed()
    {
        tmOk = true;
        causeOfPanic = null;
        log.info( "Kernel health set to OK" );
    }
}
