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

package org.neo4j.server.rrd;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ImpermanentGraphDatabase;

import javax.management.MalformedObjectNameException;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PropertyCountSampleableTest
{
    public ImpermanentGraphDatabase db;
    public PropertyCountSampleable sampleable;

    @Test
    public void emptyDbHasZeroNodesInUse() throws IOException, MalformedObjectNameException
    {
        assertThat( sampleable.getValue(), is( 0L ) ); 
    }

    @Test
    public void addANodeAndSampleableGoesUp() throws IOException, MalformedObjectNameException
    {
        addPropertyToReferenceNode( );

        assertThat( sampleable.getValue(), is( 1L ) );
    }

    private void addPropertyToReferenceNode( )
    {
        Transaction tx = db.beginTx();
        Node n = db.getReferenceNode();
        n.setProperty( "monkey", "rock!" );
        tx.success();
        tx.finish();
    }

    @Before
    public void setUp() throws Exception
    {
        db = new ImpermanentGraphDatabase();
        sampleable = new PropertyCountSampleable( db );
    }
}
