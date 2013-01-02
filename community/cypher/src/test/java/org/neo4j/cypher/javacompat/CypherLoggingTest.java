/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.javacompat;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.logging.BufferingLogger;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CypherLoggingTest {

    private ExecutionEngine engine;
    private BufferingLogger logger = new BufferingLogger();
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    @Test
    public void logging() throws Exception {
        engine.execute("START n=node(0) CREATE (foo {test:'me'}) RETURN n");
        engine.execute("START n=node(*) RETURN n");

        assertEquals(
                "START n=node(0) CREATE (foo {test:'me'}) RETURN n" + LINE_SEPARATOR +
                        "START n=node(*) RETURN n" + LINE_SEPARATOR,
                logger.toString());

    }

    @Before
    public void setup() throws IOException {
        engine = new ExecutionEngine(new ImpermanentGraphDatabase(), logger);
    }
}
