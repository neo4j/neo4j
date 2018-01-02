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
package org.neo4j.jmx;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Hashtable;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static org.junit.Assert.assertEquals;

public class DescriptionTest
{
    private static GraphDatabaseService graphdb;

    @BeforeClass
    public static void startDb()
    {
        graphdb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
    }

    @AfterClass
    public static void stopDb()
    {
        if ( graphdb != null )
        {
            graphdb.shutdown();
        }
        graphdb = null;
    }

    @Test
    public void canGetBeanDescriptionFromMBeanInterface() throws Exception
    {
        assertEquals( Kernel.class.getAnnotation( Description.class ).value(), kernelMBeanInfo().getDescription() );
    }

    @Test
    public void canGetMethodDescriptionFromMBeanInterface() throws Exception
    {
        for ( MBeanAttributeInfo attr : kernelMBeanInfo().getAttributes() )
        {
            try
            {
                assertEquals(
                        Kernel.class.getMethod( "get" + attr.getName() ).getAnnotation( Description.class ).value(),
                        attr.getDescription() );
            }
            catch ( NoSuchMethodException ignored )
            {
                assertEquals(
                        Kernel.class.getMethod( "is" + attr.getName() ).getAnnotation( Description.class ).value(),
                        attr.getDescription() );
            }
        }
    }

    private MBeanInfo kernelMBeanInfo() throws Exception
    {
        Kernel kernel = ((GraphDatabaseAPI) graphdb).getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class );
        ObjectName query = kernel.getMBeanQuery();
        Hashtable<String, String> properties = new Hashtable<>( query.getKeyPropertyList() );
        properties.put( "name", Kernel.NAME );
        return getPlatformMBeanServer().getMBeanInfo( new ObjectName( query.getDomain(), properties ) );
    }
}
