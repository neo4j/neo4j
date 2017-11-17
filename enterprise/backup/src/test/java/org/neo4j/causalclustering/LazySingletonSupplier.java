/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering;

import java.util.function.Supplier;

public class LazySingletonSupplier<E> implements Supplier<E>
{
    private final Supplier<E> singletonSupplier;

    private E instance;

    public LazySingletonSupplier( Supplier<E> singletonSupplier )
    {
        this.singletonSupplier = singletonSupplier;
    }

    @Override
    public E get()
    {
        if ( instance == null )
        {
            instance = singletonSupplier.get();
        }
        return instance;
    }
}
