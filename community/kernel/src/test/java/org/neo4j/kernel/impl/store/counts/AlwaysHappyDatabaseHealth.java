/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.Log;

public class AlwaysHappyDatabaseHealth extends DatabaseHealth
{

    public AlwaysHappyDatabaseHealth( DatabasePanicEventGenerator dbpe, Log log )
    {
        super( dbpe, log );
    }

    public AlwaysHappyDatabaseHealth()
    {
        super( null, null );
    }

    @Override
    public <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> panicDisguise ) throws EXCEPTION
    {
    }

    @Override
    public void panic( Throwable cause )
    {
    }

    @Override
    public boolean isHealthy()
    {
        return true;
    }

    @Override
    public void healed()
    {
    }
}
