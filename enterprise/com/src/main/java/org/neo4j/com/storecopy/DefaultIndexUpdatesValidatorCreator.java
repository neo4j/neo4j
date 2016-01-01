/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.com.storecopy;

import org.neo4j.function.Function;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.OnlineIndexUpdatesValidator;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;

import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.BATCHED;

class DefaultIndexUpdatesValidatorCreator implements
        Function<DependencyResolver,IndexUpdatesValidator>
{
    @Override
    public IndexUpdatesValidator apply( DependencyResolver resolver )
    {
        NeoStore neoStore = resolver.resolveDependency( NeoStoreProvider.class ).evaluate();
        KernelHealth kernelHealth = resolver.resolveDependency( KernelHealth.class );
        IndexingService indexing = resolver.resolveDependency( IndexingService.class );
        PropertyLoader propertyLoader = new PropertyLoader( neoStore );
        return new OnlineIndexUpdatesValidator( neoStore, kernelHealth, propertyLoader, indexing, BATCHED );
    }
}
