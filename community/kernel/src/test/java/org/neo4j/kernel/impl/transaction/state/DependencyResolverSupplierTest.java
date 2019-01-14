/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.state;


import org.junit.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager.DependencyResolverSupplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DependencyResolverSupplierTest
{
    @Test
    public void shouldReturnTheDependencyResolveFromTheRegisteredDatasource()
    {
        // given
        DataSourceManager dataSourceManager = new DataSourceManager();
        DependencyResolverSupplier supplier = new DependencyResolverSupplier( dataSourceManager );
        NeoStoreDataSource neoStoreDataSource = mock( NeoStoreDataSource.class );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( neoStoreDataSource.getDependencyResolver() ).thenReturn( dependencyResolver );

        // when
        dataSourceManager.register( neoStoreDataSource );

        // then
        assertEquals( dependencyResolver, supplier.get() );
    }

    @Test
    public void shouldReturnNullIfDataSourceHasBeenUnregistered()
    {
        // given
        DataSourceManager dataSourceManager = new DataSourceManager();
        DependencyResolverSupplier supplier = new DependencyResolverSupplier( dataSourceManager );
        NeoStoreDataSource neoStoreDataSource = mock( NeoStoreDataSource.class );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( neoStoreDataSource.getDependencyResolver() ).thenReturn( dependencyResolver );
        dataSourceManager.register( neoStoreDataSource );

        // when
        dataSourceManager.unregister( neoStoreDataSource );

        // then
        assertEquals( null, supplier.get() );
    }
}
