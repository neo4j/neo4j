/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.management;

import java.util.Map;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

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
    public static EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( ManagementBeansTest.class );
    private static GraphDatabaseAPI graphDb;

    @BeforeClass
    public static synchronized void startGraphDb()
    {
        graphDb = dbRule.getGraphDatabaseAPI();
    }

    @Test
    public void canAccessKernelBean() throws Exception
    {
        Kernel kernel = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Kernel.class );
        assertNotNull( "kernel bean is null", kernel );
        assertNotNull( "MBeanQuery of kernel bean is null", kernel.getMBeanQuery() );
    }

    @Test
    public void canAccessPrimitivesBean() throws Exception
    {
        Primitives primitives = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Primitives.class );
        assertNotNull( "primitives bean is null", primitives );
        primitives.getNumberOfNodeIdsInUse();
    }

    @Test
    public void canListAllBeans() throws Exception
    {
        Neo4jManager manager = getManager();
        assertTrue( "No beans returned", manager.allBeans().size() > 0 );
    }

    @Test
    public void canGetConfigurationParameters() throws Exception
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
    public void canGetLockManagerBean() throws Exception
    {
        assertNotNull( getManager().getLockManagerBean() );
    }

    @Test
    public void canIndexSamplingManagerBean() throws Exception
    {
        assertNotNull( getManager().getIndexSamplingManagerBean() );
    }

    @Test
    public void canGetMemoryMappingBean() throws Exception
    {
        assertNotNull( getManager().getMemoryMappingBean() );
    }

    @Test
    public void canGetPrimitivesBean() throws Exception
    {
        assertNotNull( getManager().getPrimitivesBean() );
    }

    @Test
    public void canGetStoreFileBean() throws Exception
    {
        assertNotNull( getManager().getStoreFileBean() );
    }

    @Test
    public void canGetTransactionManagerBean() throws Exception
    {
        assertNotNull( getManager().getTransactionManagerBean() );
    }

    @Test
    public void canGetPageCacheBean() throws Exception
    {
        assertNotNull( getManager().getPageCacheBean() );
    }

    @Test
    public void canAccessMemoryMappingCompositData() throws Exception
    {
        assertNotNull( "MemoryPools is null", getManager().getMemoryMappingBean().getMemoryPools() );
    }
}
