/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.impl.api.state.ExplicitIndexTransactionStateImpl;
import org.neo4j.kernel.impl.index.IndexConfigStore;

public class ExplicitIndexTransactionStateProvider
{
    private final IndexConfigStore indexConfigStore;
    private final ExplicitIndexProvider explicitIndexProviderLookup;

    public ExplicitIndexTransactionStateProvider( IndexConfigStore indexConfigStore, ExplicitIndexProvider explicitIndexProviderLookup )
    {
        this.indexConfigStore = indexConfigStore;
        this.explicitIndexProviderLookup = explicitIndexProviderLookup;
    }

    public ExplicitIndexTransactionState createNewExplicitIndexTransactionState()
    {
        return new CachingExplicitIndexTransactionState( new ExplicitIndexTransactionStateImpl( indexConfigStore, explicitIndexProviderLookup ) );
    }
}
