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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.collection.pool.LinkedQueuePool;
import org.neo4j.kernel.impl.store.NeoStore;

public class NeoStoreTransactionContextSupplier extends LinkedQueuePool<NeoStoreTransactionContext>
{
    private final NeoStore neoStore;

    public NeoStoreTransactionContextSupplier( NeoStore neoStore )
    {
        super( Runtime.getRuntime().availableProcessors() * 2, null,
                new CheckStrategy.TimeoutCheckStrategy( 1000 ),
                new Monitor.Adapter<>() );
        this.neoStore = neoStore;
    }

    @Override
    protected NeoStoreTransactionContext create()
    {
        return new NeoStoreTransactionContext( this, neoStore );
    }
}
