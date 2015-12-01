/*
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
package org.neo4j.kernel;

import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.logging.Log;

import static org.neo4j.helpers.Exceptions.withCause;

public class DatabaseHealth
{
    private static final String panicMessage = "Database has panicked, please perform restart and recover";

    // Keep that cozy name for legacy purposes
    private volatile boolean dbOK = true; // TODO rather skip volatile if possible here.
    private final KernelPanicEventGenerator kpe;
    private final Log log;
    private Throwable causeOfPanic;

    public DatabaseHealth( KernelPanicEventGenerator kpe, Log log )
    {
        this.kpe = kpe;
        this.log = log;
    }

    /**
     * Asserts that the kernel is in good health. If that is not the case then the cause of the
     * unhealthy state is wrapped in an exception of the given type, i.e. the panic disguise.
     *
     * @param panicDisguise the cause of the unhealthy state wrapped in an exception of this type.
     * @throws EXCEPTION exception type to wrap cause in.
     */
    public <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> panicDisguise ) throws EXCEPTION
    {
        if ( !dbOK )
        {
            EXCEPTION exception;
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
                        " was requested to be thrown but that proved impossible", e );
            }
            throw exception;
        }
    }

    public void panic( Throwable cause )
    {
        if ( !dbOK )
        {
            return;
        }

        if ( cause == null )
        {
            throw new IllegalArgumentException( "Must provide a cause for the database panic" );
        }
        this.causeOfPanic = cause;
        this.dbOK = false;
        // The "DB panic" string used for grep:ability
        log.error( "DB panic: . " + panicMessage, cause );
        kpe.generateEvent( ErrorState.TX_MANAGER_NOT_OK, causeOfPanic );
    }

    public boolean isHealthy()
    {
        return dbOK;
    }

    public Throwable getCauseOfPanic()
    {
        if ( dbOK )
        {
            throw new IllegalStateException( "Database is healthy, no cause of panic found" );
        }
        return causeOfPanic;
    }

    public void healed()
    {
        dbOK = true;
        causeOfPanic = null;
        log.info( "Database health set to OK" );
    }
}
