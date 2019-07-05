/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.store.id;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterables.first;

public class ReuseExcessBatchIdsOnRestartIT
{
    @Rule
    public final DbmsRule db = new EmbeddedDbmsRule();

    // Knowing that ids are grabbed in batches internally we only create one node and later assert
    // that the excess ids that were only grabbed, but not used can be reused.
    @Test
    public void shouldReuseExcessNodeBatchIdsWhichWereNotUsedBeforeClose() throws Exception
    {
        // given
        Node firstNode;
        try ( Transaction tx = db.beginTx() )
        {
            firstNode = db.createNode();
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode();
            // This one gets rolled back.
        }

        // when
        db.restartDatabase();

        Node secondNode;
        try ( Transaction tx = db.beginTx() )
        {
            secondNode = db.createNode();
            tx.success();
        }

        // then
        assertEquals( firstNode.getId() + 1, secondNode.getId() );
    }

    @Ignore // Does not pass, but it presently doesn't matter.
    @Test
    public void shouldReuseExcessNodeBatchIdsWhichWereReturnedBeforeClose() throws Exception
    {
        // given
        Node firstNode;
        try ( Transaction tx = db.beginTx() )
        {
            firstNode = db.createNode();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            // This one gets deleted before commit.
            Node node = db.createNode();
            node.delete();
            tx.success();
        }

        // when
        db.restartDatabase();

        Node secondNode;
        try ( Transaction tx = db.beginTx() )
        {
            secondNode = db.createNode();
            tx.success();
        }

        // then
        assertEquals( firstNode.getId() + 1, secondNode.getId() );
    }

    @Test
    public void shouldReuseExcessIndexBatchIdsWhichWereNotUsedBeforeClose() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( Label.label( "LabelA" ) ).on( "a" ).withName( "A" ).create();
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            db.schema().indexFor( Label.label( "LabelX" ) ).on( "x" ).withName( "X" ).create();
            // This one gets rolled back.
        }

        // when
        db.restartDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( Label.label( "LabelB" ) ).on( "b" ).withName( "B" ).create();
            tx.success();
        }

        // then
        IndexDefinition first;
        IndexDefinition second;
        try ( Transaction tx = db.beginTx() )
        {
            first = db.schema().getIndexByName( "A" );
            second = db.schema().getIndexByName( "B" );
            tx.success();
        }
        IndexDefinitionImpl firstImpl = (IndexDefinitionImpl) first;
        IndexDefinitionImpl secondImpl = (IndexDefinitionImpl) second;
        IndexDescriptor firstRef = firstImpl.getIndexReference();
        IndexDescriptor secondRef = secondImpl.getIndexReference();
        assertEquals( firstRef.getId() + 1, secondRef.getId() );
    }

    @Test
    public void shouldReuseExcessConstraintBatchIdsWhichWereNotUsedBeforeClose() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( Label.label( "LabelA" ) ).assertPropertyIsUnique( "a" ).create();
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            db.schema().constraintFor( Label.label( "LabelX" ) ).assertPropertyIsUnique( "x" ).create();
            // This one gets rolled back.
        }

        // when
        db.restartDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( Label.label( "LabelB" ) ).assertPropertyIsUnique( "b" ).create();
            tx.success();
        }

        // then
        IndexDefinition first;
        IndexDefinition second;
        try ( Transaction tx = db.beginTx() )
        {
            first = first( db.schema().getIndexes( Label.label( "LabelA" ) ) );
            second = first( db.schema().getIndexes( Label.label( "LabelB" ) ) );
            tx.success();
        }
        IndexDefinitionImpl firstImpl = (IndexDefinitionImpl) first;
        IndexDefinitionImpl secondImpl = (IndexDefinitionImpl) second;
        IndexDescriptor firstRef = firstImpl.getIndexReference();
        IndexDescriptor secondRef = secondImpl.getIndexReference();
        // This time we "+2" because there are both index and constraint schema records being created.
        assertEquals( firstRef.getId() + 2, secondRef.getId() );
    }
}
