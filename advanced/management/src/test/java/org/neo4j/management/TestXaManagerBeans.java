/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.management;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.management.impl.XaManagerBean;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.*;

public class TestXaManagerBeans
{
    private GraphDatabaseAPI graphDb;
    private XaManager xaManager;
    private TargetDirectory dir = TargetDirectory.forTest( getClass() );

    @Before
    public synchronized void startGraphDb()
    {
        graphDb = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(dir.cleanDirectory( "test" ).getAbsolutePath() );
        xaManager = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( XaManager.class );
    }

    @After
    public synchronized void stopGraphDb()
    {
        if ( graphDb != null )
        {
            graphDb.shutdown();
        }
        graphDb = null;
    }

    @Test
    public void canAccessXaManagerBean() throws Exception
    {
        assertNotNull( "no XA manager bean", xaManager );
        assertTrue( "no XA resources", xaManager.getXaResources().length > 0 );
    }

    @Test
    public void hasAllXaManagerBeans()
    {
        for ( XaDataSource xaDataSource : graphDb.getDependencyResolver().resolveDependency( XaDataSourceManager.class )
                .getAllRegisteredDataSources() )
        {
            XaResourceInfo info = getByName( xaDataSource.getName() );
            assertEquals( "wrong branchid for XA data source " + xaDataSource.getName(),
                    XaManagerBean.toHexString( xaDataSource.getBranchId() ), info.getBranchId() );
            assertEquals( "wrong log version for XA data source " + xaDataSource.getName(),
                    xaDataSource.getCurrentLogVersion(), info.getLogVersion() );
            assertEquals( "wrong last tx ID for XA data source " + xaDataSource.getName(),
                    xaDataSource.getLastCommittedTxId(), info.getLastTxId() );
        }
    }

    private XaResourceInfo getByName( String name )
    {
        for ( XaResourceInfo xaResourceInfo : xaManager.getXaResources() )
        {
            if ( name.equals( xaResourceInfo.getName() ) )
            {
                return xaResourceInfo;
            }
        }
        fail( "no such XA resource: " + name );
        return null;
    }

}
