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
package org.neo4j.kernel.impl.transaction.log;

public class ControlledParkStrategy implements ParkStrategy
{
    private volatile boolean idle;

    @Override
    public void park( Thread thread )
    {
        idle = true;
        await( false );
    }

    @Override
    public void unpark( Thread thread )
    {
        idle = false;
    }

    public void awaitIdle()
    {
        await( true );
    }

    private void await( boolean idle )
    {
        while ( this.idle != idle )
        {
            try
            {
                Thread.sleep( 1 );
            }
            catch ( InterruptedException e )
            {   // Do continue
                Thread.interrupted();
            }
        }
    }
}
