/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples;

import java.util.Date;

import javax.management.ObjectName;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.jmx.JmxUtils;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class JmxDocTest
{
    @Test
    public void readJmxProperties()
    {
        GraphDatabaseService graphDbService = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            Date startTime = getStartTimeFromManagementBean( graphDbService );
            Date now = new Date();
            assertTrue( startTime.before( now ) || startTime.equals( now ) );
        }
        finally
        {
            graphDbService.shutdown();
        }
    }

    @Test
    public void properErrorOnNonStandardGraphDatabase()
    {
        GraphDatabaseService graphDbService = mock(GraphDatabaseService.class);
        try
        {
            getStartTimeFromManagementBean( graphDbService );
            fail("Expected error");
        }
        catch(IllegalArgumentException e)
        {
            assertEquals("Can only resolve object names for embedded Neo4j database " +
                    "instances, eg. instances created by GraphDatabaseFactory or HighlyAvailableGraphDatabaseFactory.",
                    e.getMessage());
        }
    }

    // START SNIPPET: getStartTime
    private static Date getStartTimeFromManagementBean(
            GraphDatabaseService graphDbService )
    {
        ObjectName objectName = JmxUtils.getObjectName( graphDbService, "Kernel" );
        Date date = JmxUtils.getAttribute( objectName, "KernelStartTime" );
        return date;
    }
    // END SNIPPET: getStartTime
}
