/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

package org.neo4j.server.webadmin.console;

import org.junit.Test;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappedDatabase;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class CypherSessionDocTest
{
    @Test
    public void shouldReturnASingleNode() throws Throwable
    {
        AbstractGraphDatabase graphdb = (AbstractGraphDatabase) new TestGraphDatabaseFactory().newImpermanentDatabase();
        Database database = new WrappedDatabase( graphdb );
        CypherExecutor executor = new CypherExecutor( database, StringLogger.DEV_NULL );
        executor.start();
        try
        {
            CypherSession session = new CypherSession( executor, DevNullLoggingService.DEV_NULL );
            Pair<String, String> result = session.evaluate( "create (a) return a" );
            assertThat( result.first(), containsString( "Node[0]" ) );
        }
        finally
        {
            graphdb.shutdown();
        }
    }
}
