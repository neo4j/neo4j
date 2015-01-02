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
package org.neo4j.kernel.impl;

import org.junit.Test;

import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class XaDataSourceManagerTest
{
    @Test
    public void shouldNotAccessResourcesDuringShutdown()
    {
        XaDataSourceManager manager = new XaDataSourceManager(null);
        manager.registerDataSource( new DataSourceThatRefusesAccessToResources( "the-data-source" ) );
        manager.unregisterDataSource( "the-data-source" );
    }

    private static class DataSourceThatRefusesAccessToResources extends XaDataSource
    {
        public DataSourceThatRefusesAccessToResources( String name )
        {
            super( new byte[]{}, name );
        }

        @Override
        public XaConnection getXaConnection()
        {
            throw new RuntimeException( "Should not access connection during shutdown." );
        }

        @Override
        public void init() throws Throwable { }
        @Override
        public void start() throws Throwable { }
        @Override
        public void stop() throws Throwable { }
        @Override
        public void shutdown() throws Throwable { }
    }
}
