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
package org.neo4j.kernel.impl.store.id;

import org.neo4j.function.Consumer;
import org.neo4j.function.Predicate;
import org.neo4j.function.Supplier;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;

class BufferingIdGenerator extends IdGenerator.Delegate
{
    private DelayedBuffer<KernelTransactionsSnapshot> buffer;

    public BufferingIdGenerator( IdGenerator delegate )
    {
        super( delegate );
    }

    void initialize( Supplier<KernelTransactionsSnapshot> boundaries,
            Predicate<KernelTransactionsSnapshot> safeThreshold )
    {
        buffer = new DelayedBuffer<>( boundaries, safeThreshold, 10_000, new Consumer<long[]>()
        {
            @Override
            public void accept( long[] freedIds )
            {
                for ( long id : freedIds )
                {
                    actualFreeId( id );
                }
            }
        } );
    }

    private void actualFreeId( long id )
    {
        super.freeId( id );
    }

    @Override
    public void freeId( long id )
    {
        buffer.offer( id );
    }

    void maintenance()
    {
        buffer.maintenance();
    }

    void clear()
    {
        buffer.clear();
    }

    @Override
    public void close()
    {
        if ( buffer != null )
        {
            buffer.close();
        }
        super.close();
    }
}
