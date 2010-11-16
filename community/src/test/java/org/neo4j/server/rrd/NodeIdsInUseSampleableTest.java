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

import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ImpermanentGraphDatabase;

import javax.management.MalformedObjectNameException;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class NodeIdsInUseSampleableTest
{
    @Test
    public void emptyDbHasZeroNodesInUse() throws IOException, MalformedObjectNameException
    {
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase();
        NodeIdsInUseSampleable sampleable = new NodeIdsInUseSampleable( db );

        assertThat( sampleable.getValue(), is( 1L ) ); //Reference node is always created in empty dbs
    }

    @Test
    public void addANodeAndSampleableGoesUp() throws IOException, MalformedObjectNameException
    {
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase();
        NodeIdsInUseSampleable sampleable = new NodeIdsInUseSampleable( db );
        createNode( db );

        assertThat( sampleable.getValue(), is( 2L ) ); 
    }

    private void createNode( ImpermanentGraphDatabase db )
    {
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
    }
}
