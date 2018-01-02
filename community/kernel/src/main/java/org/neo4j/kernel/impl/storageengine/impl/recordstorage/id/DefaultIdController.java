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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage.id;


import java.util.function.Supplier;

import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Default implementation of {@link IdController}.
 * Do not add any additional possibilities or functionality. Wraps provided {@link IdGeneratorFactory}.
 */
public class DefaultIdController extends LifecycleAdapter implements IdController
{
    public DefaultIdController()
    {
    }

    @Override
    public void clear()
    {
    }

    @Override
    public void maintenance()
    {
    }

    @Override
    public void initialize( Supplier<KernelTransactionsSnapshot> transactionsSnapshotSupplier )
    {
    }
}
