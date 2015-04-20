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
package org.neo4j.kernel.impl.cache;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.RepeatRule.Repeat;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class RelationshipChainPositionPoisoningTest
{
    /**
     * Problem previously relied on order that items were iterated over a VersionedHashMap,
     * which for ids 0-9 will be ordered, although not so for higher ids. That's why this test
     * is repeated, so that all orderings are tested.
     */
    @Repeat( times = 10 )
    @Test
    public void shouldPatchNodeCacheWithCorrectRelationshipAfterTwoDeleted() throws Exception
    {
        // GIVEN a relationship chain like 4->3->2->1->0
        Node node = createNode();
        Relationship[] rels = new Relationship[5];
        for ( int i = 0; i < rels.length; i++ )
        {
            rels[i] = createRelationship( node );
        }
        db.clearCache();

        try ( Transaction tx = db.beginTx() )
        {
            // and GIVEN relationship chain position is at 2 (see chain layout above).
            // This will be the case since grab size is 2.
            node.getRelationships().iterator().next();

            // WHEN
            // deleting relationships 2 and 1
            rels[2].delete();
            rels[1].delete();
            tx.success();
        }

        // THEN continuing loading relationships should see the last one (0) as well.
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 3, count( node.getRelationships() ) );
        }
    }

    private Relationship createRelationship( Node node )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.success();
            return relationship;
        }
    }

    private Node createNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            tx.success();
            return node;
        }
    }

    public final @Rule RepeatRule repeater = new RepeatRule();
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.cache_type, StrongCacheProvider.NAME );
            builder.setConfig( GraphDatabaseSettings.relationship_grab_size, "2" );
        }
    };
}
