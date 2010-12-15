/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.console;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.server.database.Database;

public class GremlinSessionTest
{
    private ScriptSession session;
    private Database database;

    @Test
    public void retrievesTheReferenceNode()
    {
        String result = session.evaluate( "$_" );

        assertThat( result, is( "v[0]" ) );
    }

    @Test
    public void canCreateNodesInGremlinLand()
    {
        String result = session.evaluate( "g:add-v()" );

        assertThat( result, is( "v[1]" ) );
    }

    @Before
    public void setUp() throws Exception
    {
        this.database = new Database( new ImpermanentGraphDatabase( "target/tempdb" ) );
        session = new GremlinSession( database );
    }

    @After
    public void shutdownDatabase()
    {
        this.database.shutdown();
    }
}

