/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.Primitives;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ManagementBeansTest
{
    @ClassRule
    public static EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule();
    private static GraphDatabaseAPI graphDb;

    @BeforeClass
    public static synchronized void startGraphDb()
    {
        graphDb = dbRule.getGraphDatabaseAPI();
    }

    @Test
    public void canAccessKernelBean()
    {
        Kernel kernel = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Kernel.class );
        assertNotNull( "kernel bean is null", kernel );
        assertNotNull( "MBeanQuery of kernel bean is null", kernel.getMBeanQuery() );
    }

    @Test
    public void canAccessPrimitivesBean()
    {
        Primitives primitives = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Primitives.class );
        assertNotNull( "primitives bean is null", primitives );
        primitives.getNumberOfNodeIdsInUse();
    }

    @Test
    public void canListAllBeans()
    {
        Neo4jManager manager = getManager();
        assertTrue( "No beans returned", manager.allBeans().size() > 0 );
    }

    @Test
    public void canGetConfigurationParameters()
    {
        Neo4jManager manager = getManager();
        Map<String, Object> configuration = manager.getConfiguration();
        assertTrue( "No configuration returned", configuration.size() > 0 );
    }

    private Neo4jManager getManager()
    {
        return new Neo4jManager( graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Kernel.class ) );
    }

    @Test
    public void canGetLockManagerBean()
    {
        assertNotNull( getManager().getLockManagerBean() );
    }

    @Test
    public void canIndexSamplingManagerBean()
    {
        assertNotNull( getManager().getIndexSamplingManagerBean() );
    }

    @Test
    public void canGetMemoryMappingBean()
    {
        assertNotNull( getManager().getMemoryMappingBean() );
    }

    @Test
    public void canGetPrimitivesBean()
    {
        assertNotNull( getManager().getPrimitivesBean() );
    }

    @Test
    public void canGetStoreFileBean()
    {
        assertNotNull( getManager().getStoreFileBean() );
    }

    @Test
    public void canGetTransactionManagerBean()
    {
        assertNotNull( getManager().getTransactionManagerBean() );
    }

    @Test
    public void canGetPageCacheBean()
    {
        assertNotNull( getManager().getPageCacheBean() );
    }

    @Test
    public void canAccessMemoryMappingCompositData()
    {
        assertNotNull( "MemoryPools is null", getManager().getMemoryMappingBean().getMemoryPools() );
    }
}
