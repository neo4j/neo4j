/*
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
package org.neo4j.server.logging;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static org.neo4j.server.logging.JettyLoggerAdapter.safeFormat;

public class JettyLoggerAdapterTest
{
    @Test
    public void testSafeFormat() throws Exception
    {
        assertEquals( "Failed to format message: ?", safeFormat( "%" ) );
        assertEquals( "Failed to format message: Program?20Files", safeFormat( "Program%20Files" ) );
        assertEquals( "Failed to format message: x arg1: ?", safeFormat( "x", "%" ) );
        assertEquals( "Failed to format message: x arg1: y arg2: null", safeFormat( "x", "y", null ) );
    }
}
