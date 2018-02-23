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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import javax.annotation.Resource;

import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.Primitives;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.EmbeddedDatabaseExtension;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( EmbeddedDatabaseExtension.class )
class ManagementBeansTest
{
    @Resource
    private EmbeddedDatabaseRule dbRule;
    private static GraphDatabaseAPI graphDb;

    @BeforeEach
    void setUp()
    {
        graphDb = dbRule.getGraphDatabaseAPI();
    }

    @Test
    void canAccessKernelBean()
    {
        Kernel kernel = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Kernel.class );
        assertNotNull( kernel, "kernel bean is null" );
        assertNotNull( kernel.getMBeanQuery(), "MBeanQuery of kernel bean is null" );
    }

    @Test
    void canAccessPrimitivesBean()
    {
        Primitives primitives = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Primitives.class );
        assertNotNull( primitives, "primitives bean is null" );
        primitives.getNumberOfNodeIdsInUse();
    }

    @Test
    void canListAllBeans()
    {
        Neo4jManager manager = getManager();
        assertTrue( manager.allBeans().size() > 0, "No beans returned" );
    }

    @Test
    void canGetConfigurationParameters()
    {
        Neo4jManager manager = getManager();
        Map<String, Object> configuration = manager.getConfiguration();
        assertTrue( configuration.size() > 0, "No configuration returned" );
    }

    private Neo4jManager getManager()
    {
        return new Neo4jManager( graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Kernel.class ) );
    }

    @Test
    void canGetLockManagerBean()
    {
        assertNotNull( getManager().getLockManagerBean() );
    }

    @Test
    void canIndexSamplingManagerBean()
    {
        assertNotNull( getManager().getIndexSamplingManagerBean() );
    }

    @Test
    void canGetMemoryMappingBean()
    {
        assertNotNull( getManager().getMemoryMappingBean() );
    }

    @Test
    void canGetPrimitivesBean()
    {
        assertNotNull( getManager().getPrimitivesBean() );
    }

    @Test
    void canGetStoreFileBean()
    {
        assertNotNull( getManager().getStoreFileBean() );
    }

    @Test
    void canGetTransactionManagerBean()
    {
        assertNotNull( getManager().getTransactionManagerBean() );
    }

    @Test
    void canGetPageCacheBean()
    {
        assertNotNull( getManager().getPageCacheBean() );
    }

    @Test
    void canAccessMemoryMappingCompositData()
    {
        assertNotNull( getManager().getMemoryMappingBean().getMemoryPools(), "MemoryPools is null" );
    }
}
