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
package org.neo4j.kernel.impl.index;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.index.IndexManager.PROVIDER;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.test.ImpermanentGraphDatabase;

public class TestIndexImplOnNeo
{
    private ImpermanentGraphDatabase db;

    @Before
    public void doBefore() throws Exception
    {
        db = new ImpermanentGraphDatabase();
    }

    @After
    public void doAfter() throws Exception
    {
        db.shutdown();
    }
    
    @Test
    public void createIndexWithProviderThatUsesNeoAsDataSource() throws Exception
    {
        Index<Node> index = db.index().forNodes( "inneo", stringMap( PROVIDER, "test-dummy-neo-index" ) );
        // Querying for "refnode" always returns the reference node for this dummy index.
        assertEquals( db.getReferenceNode(), index.get( "key", "refnode" ).getSingle() );
        // Querying for something other than "refnode" returns null for this dummy index.
        assertEquals( 0, count( (Iterable<Node>) index.get( "key", "something else" ) ) );
    }
}
