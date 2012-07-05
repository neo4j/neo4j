/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.plugins;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.server.rest.repr.formats.NullFormat;
import org.neo4j.test.ImpermanentGraphDatabase;

public class PluginManagerTest
{
    private static PluginManager manager;
    private static InternalAbstractGraphDatabase graphDb;

    @BeforeClass
    public static void loadExtensionManager() throws Exception
    {
        graphDb = new ImpermanentGraphDatabase();
        manager = new PluginManager( null, null );
    }

    @AfterClass
    public static void destroyExtensionManager()
    {
        manager = null;
        if ( graphDb != null ) graphDb.shutdown();
        graphDb = null;
    }

    @Test
    public void canGetUrisForNode() throws Exception
    {
        Map<String, List<String>> extensions = manager.getExensionsFor( GraphDatabaseService.class );
        List<String> methods = extensions.get( FunctionalTestPlugin.class.getSimpleName() );
        assertNotNull( methods );
        assertThat( methods, hasItem( FunctionalTestPlugin.GET_REFERENCE_NODE ) );
    }

    @Test
    public void canInvokeExtension() throws Exception
    {
        manager.invoke( graphDb, FunctionalTestPlugin.class.getSimpleName(), GraphDatabaseService.class,
                FunctionalTestPlugin.GET_REFERENCE_NODE, graphDb,
                new NullFormat( null, (MediaType[]) null ).readParameterList( "" ) );
    }
}
