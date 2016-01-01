/**
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
package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.util.FlyweightPool;

public class NeoStoreTransactionContextSupplier extends FlyweightPool<NeoStoreTransactionContext>
{
    private final NeoStore neoStore;

    public NeoStoreTransactionContextSupplier( NeoStore neoStore )
    {
        super( Runtime.getRuntime().availableProcessors() * 2,
                new CheckStrategy.TimeoutCheckStrategy( 1000, Clock.SYSTEM_CLOCK ), new Monitor.Adapter<>() );
        this.neoStore = neoStore;
    }

    @Override
    protected NeoStoreTransactionContext create()
    {
        return new NeoStoreTransactionContext( this, neoStore );
    }
}
