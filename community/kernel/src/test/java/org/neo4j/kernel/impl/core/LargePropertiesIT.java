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
package org.neo4j.kernel.impl.core;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.GraphTransactionRule;
import org.neo4j.test.ImpermanentDatabaseRule;

public class LargePropertiesIT
{
    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();

    @Rule
    public GraphTransactionRule tx = new GraphTransactionRule( db );

    private Node node;

    @Before
    public void createInitialNode()
    {
        node = db.getGraphDatabaseService().createNode();
    }

    @After
    public void deleteInitialNode()
    {
        node.delete();
    }
    
    @Test
    public void testLargeProperties()
    {
        byte[] bytes = new byte[10*1024*1024];
        node.setProperty( "large_array", bytes );
        node.setProperty( "large_string", new String( bytes ) );
    }
}
