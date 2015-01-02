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
package org.neo4j.kernel.impl.transaction;

import org.neo4j.kernel.impl.core.NoTransactionState;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

/**
 * This is used by transactions when Neo4j is in read-only mode. Same as NoTransactionState, but keeps a ResourceHolder.
 * Otherwise they would be created for each accessed resource in the transaction.
 */
public class ReadOnlyTransactionState
    extends NoTransactionState
{
    private PersistenceManager.ResourceHolder neoStoreTransaction;

    @Override
    public PersistenceManager.ResourceHolder getNeoStoreTransaction()
    {
        return neoStoreTransaction;
    }

    @Override
    public void setNeoStoreTransaction( PersistenceManager.ResourceHolder neoStoreTransaction )
    {
        this.neoStoreTransaction = neoStoreTransaction;
    }
}
