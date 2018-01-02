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
package org.neo4j.ext.udc;

import org.junit.Test;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class UdcSettingsIT
{
    public static final String TEST_HOST_AND_PORT = "test.ucd.neo4j.org:8080";

    @Test
    public void testUdcHostSettingIsUnchanged() throws Exception
    {
        //noinspection deprecation
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( UdcSettings.udc_host, TEST_HOST_AND_PORT )
                .newGraphDatabase();

        Config config = db.getDependencyResolver().resolveDependency( Config.class );

        assertEquals( TEST_HOST_AND_PORT, config.get( UdcSettings.udc_host ).toString() );
    }
}
