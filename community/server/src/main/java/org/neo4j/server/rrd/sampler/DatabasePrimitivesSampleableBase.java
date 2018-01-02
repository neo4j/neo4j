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
package org.neo4j.server.rrd.sampler;

import org.rrd4j.DsType;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.server.rrd.Sampleable;

public abstract class DatabasePrimitivesSampleableBase implements Sampleable
{
    private final NeoStoresSupplier neoStore;
    private final AvailabilityGuard guard;
    private double lastReadValue = 0d;

    public DatabasePrimitivesSampleableBase( NeoStoresSupplier neoStore, AvailabilityGuard guard )
    {
        if ( neoStore == null )
        {
            throw new RuntimeException( "Database sampler needs a NeoStores to work, was given null." );
        }
        this.neoStore = neoStore;
        this.guard = guard;
    }

    @Override
    public double getValue()
    {
        if ( guard.isAvailable( 0 ) )
        {
            try
            {
                lastReadValue = readValue( neoStore.get() );
            }
            catch ( Exception e )
            {
                /*
                 * oh well, this is unfortunate, perhaps the db went down after the check, so let's ignore this problem
                 * and return the old value, we'll sample again when the db is online again
                 */
            }
        }

        return lastReadValue;
    }

    protected abstract double readValue( NeoStores neoStore );

    @Override
    public DsType getType()
    {
        return DsType.GAUGE;
    }
}
