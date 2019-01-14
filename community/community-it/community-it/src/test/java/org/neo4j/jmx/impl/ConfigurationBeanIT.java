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
package org.neo4j.jmx.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.management.Attribute;
import javax.management.AttributeList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class ConfigurationBeanIT
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
    public void durationListedWithUnit()
    {
        ConfigurationBean configurationBean =
                ( (GraphDatabaseAPI) graphdb ).getDependencyResolver().resolveDependency(
                        JmxKernelExtension.class ).getSingleManagementBean( ConfigurationBean.class );

        Object v = configurationBean.getAttribute( GraphDatabaseSettings.log_queries_threshold.name() );
        assertEquals( "0ms", v );

        AttributeList attrs = configurationBean.getAttributes(
                new String[]{ GraphDatabaseSettings.log_queries_threshold.name() } );

        assertEquals( 1, attrs.size() );

        Attribute attr = (Attribute) attrs.get( 0 );

        assertEquals( "0ms", attr.getValue() );
        assertEquals( "dbms.logs.query.threshold = 0ms", attr.toString() );
    }
}
