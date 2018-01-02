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
package org.neo4j.adversaries;

import java.util.ArrayList;
import java.util.List;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public final class AdversarySignals
{
    private static final AdversarySignals instance = new AdversarySignals();

    private boolean installed;
    private List<Runnable> installedHandlers;

    private AdversarySignals()
    {
        installed = false;
        installedHandlers = new ArrayList<Runnable>();
    }

    public static AdversarySignals getInstance()
    {
        return instance;
    }

    public synchronized void installAsSIGUSR2()
    {
        if ( !installed )
        {
            Signal.handle( new Signal( "USR2" ), new SignalHandler()
            {
                @Override
                public void handle( Signal sig )
                {
                    handleSignal();
                }
            } );
            installed = true;
        }
    }

    private synchronized void handleSignal()
    {
        for ( Runnable handler : installedHandlers )
        {
            handler.run();
        }
    }

    public synchronized void setFactorWhenSignalled(
            final RandomAdversary adversary,
            final double factor )
    {
        installedHandlers.add( new Runnable()
        {
            @Override
            public void run()
            {
                adversary.setProbabilityFactor( factor );
            }
        } );
    }

    public synchronized void setAndResetFactorWhenSignalled(
            final RandomAdversary adversary,
            final double factor )
    {
        installedHandlers.add( new Runnable()
        {
            @Override
            public void run()
            {
                adversary.setAndResetProbabilityFactor( factor );
            }
        } );
    }
}
