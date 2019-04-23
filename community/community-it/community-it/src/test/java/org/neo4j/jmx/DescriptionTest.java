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
package org.neo4j.jmx;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Hashtable;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.jmx.impl.JmxExtension;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class DescriptionTest
{
    private static GraphDatabaseService graphdb;
    private static DatabaseManagementService managementService;

    @BeforeAll
    static void startDb()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        graphdb = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterAll
    static void stopDb()
    {
        if ( graphdb != null )
        {
            managementService.shutdown();
        }
        graphdb = null;
    }

    @Test
    void canGetBeanDescriptionFromMBeanInterface() throws Exception
    {
        Assertions.assertEquals( Kernel.class.getAnnotation( Description.class ).value(), kernelMBeanInfo().getDescription() );
    }

    @Test
    void canGetMethodDescriptionFromMBeanInterface() throws Exception
    {
        for ( MBeanAttributeInfo attr : kernelMBeanInfo().getAttributes() )
        {
            try
            {
                Assertions.assertEquals(
                        Kernel.class.getMethod( "get" + attr.getName() ).getAnnotation( Description.class ).value(),
                        attr.getDescription() );
            }
            catch ( NoSuchMethodException ignored )
            {
                Assertions.assertEquals(
                        Kernel.class.getMethod( "is" + attr.getName() ).getAnnotation( Description.class ).value(),
                        attr.getDescription() );
            }
        }
    }

    private MBeanInfo kernelMBeanInfo() throws Exception
    {
        Kernel kernel = ((GraphDatabaseAPI) graphdb).getDependencyResolver().resolveDependency( JmxExtension
                .class ).getSingleManagementBean( Kernel.class );
        ObjectName query = kernel.getMBeanQuery();
        Hashtable<String, String> properties = new Hashtable<>( query.getKeyPropertyList() );
        properties.put( "name", Kernel.NAME );
        return getPlatformMBeanServer().getMBeanInfo( new ObjectName( query.getDomain(), properties ) );
    }
}
