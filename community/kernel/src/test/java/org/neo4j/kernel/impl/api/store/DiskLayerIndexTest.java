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
package org.neo4j.kernel.impl.api.store;

import java.util.Set;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.index.IndexDescriptor;

import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * Test read-access to committed index data.
 */
public class DiskLayerIndexTest extends DiskLayerTest
{
    @Test
    public void should_find_nodes_with_given_label_and_property_via_index() throws Exception
    {
        // GIVEN
        IndexDescriptor descriptor = new IndexDescriptor( 0, 0 );
        createIndexAndAwaitOnline( label1, propertyKey );

        String name = "Mr. Taylor";
        Node mrTaylor = createLabeledNode( db, map( propertyKey, name ), label1 );
        try ( Transaction ignored = db.beginTx() )
        {
            // WHEN
            Set<Long> foundNodes = asUniqueSet( disk.nodesGetFromIndexSeek( state, descriptor, name ) );

            // THEN
            assertEquals( asSet( mrTaylor.getId() ), foundNodes );
        }
    }
}
