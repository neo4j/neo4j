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
package org.neo4j.index.impl.lucene;

import org.junit.Rule;
import org.junit.Test;

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableRelationshipIndex;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class AutoIndexerTest
{
    public final @Rule DatabaseRule db = new EmbeddedDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            super.configure( builder );
            builder.setConfig( GraphDatabaseSettings.relationship_keys_indexable, "Type" );
            builder.setConfig( GraphDatabaseSettings.relationship_auto_indexing, "true" );
        }
    };

    @Test
    public void shouldNotSeeDeletedRelationshipWhenQueryingWithStartAndEndNode()
    {
        RelationshipType type = MyRelTypes.TEST;
        long startId;
        long endId;
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            Node start = db.createNode();
            Node end = db.createNode();
            startId = start.getId();
            endId = end.getId();
            rel = start.createRelationshipTo( end, type );
            rel.setProperty( "Type", type.name() );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            ReadableRelationshipIndex autoRelationshipIndex =
                    db.index().getRelationshipAutoIndexer().getAutoIndex();
            Node start = db.getNodeById( startId );
            Node end = db.getNodeById( endId );
            IndexHits<Relationship> hits = autoRelationshipIndex.get( "Type", type.name(), start, end );
            assertEquals( 1, count( (Iterator<Relationship>)hits ) );
            assertEquals( 1, hits.size() );
            rel.delete();
            autoRelationshipIndex = db.index().getRelationshipAutoIndexer().getAutoIndex();
            hits = autoRelationshipIndex.get( "Type", type.name(), start, end );
            assertEquals( 0, count( (Iterator<Relationship>)hits ) );
            assertEquals( 0, hits.size() );
            tx.success();
        }
    }
}
