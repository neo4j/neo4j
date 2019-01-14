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
package org.neo4j.index.timeline;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.index.lucene.LuceneTimeline;
import org.neo4j.index.lucene.TimelineIndex;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asCollection;

public class TestTimeline
{
    private GraphDatabaseService db;

    @Before
    public void before()
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
    }

    @After
    public void after()
    {
        db.shutdown();
    }

    private interface EntityCreator<T extends PropertyContainer>
    {
        T create();
    }

    private EntityCreator<PropertyContainer> nodeCreator = new EntityCreator<PropertyContainer>()
    {
        @Override
        public Node create()
        {
            return db.createNode();
        }
    };

    private EntityCreator<PropertyContainer> relationshipCreator = new EntityCreator<PropertyContainer>()
    {
        private final RelationshipType type = RelationshipType.withName( "whatever" );

        @Override
        public Relationship create()
        {
            return db.createNode().createRelationshipTo( db.createNode(), type );
        }
    };

    private TimelineIndex<PropertyContainer> nodeTimeline()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( "timeline" );
            tx.success();
            return new LuceneTimeline( db, nodeIndex );
        }
    }

    private TimelineIndex<PropertyContainer> relationshipTimeline()
    {
        try ( Transaction tx = db.beginTx() )
        {
            RelationshipIndex relationshipIndex = db.index().forRelationships( "timeline" );
            tx.success();
            return new LuceneTimeline( db, relationshipIndex );
        }
    }

    private LinkedList<Pair<PropertyContainer, Long>> createTimestamps( EntityCreator<PropertyContainer> creator,
            TimelineIndex<PropertyContainer> timeline, long... timestamps )
    {
        try ( Transaction tx = db.beginTx() )
        {
            LinkedList<Pair<PropertyContainer,Long>> result = new LinkedList<>();
            for ( long timestamp : timestamps )
            {
                result.add( createTimestampedEntity( creator, timeline, timestamp ) );
            }
            tx.success();
            return result;
        }
    }

    private Pair<PropertyContainer, Long> createTimestampedEntity( EntityCreator<PropertyContainer> creator,
            TimelineIndex<PropertyContainer> timeline, long timestamp )
    {
        PropertyContainer entity = creator.create();
        timeline.add( entity, timestamp );
        return Pair.of( entity, timestamp );
    }

    private List<PropertyContainer> sortedEntities( LinkedList<Pair<PropertyContainer, Long>> timestamps, final boolean reversed )
    {
        List<Pair<PropertyContainer, Long>> sorted = new ArrayList<>( timestamps );
        sorted.sort( ( o1, o2 ) -> !reversed ? o1.other().compareTo( o2.other() ) : o2.other().compareTo( o1.other() ) );

        List<PropertyContainer> result = new ArrayList<>();
        for ( Pair<PropertyContainer, Long> timestamp : sorted )
        {
            result.add( timestamp.first() );
        }
        return result;
    }

    // ======== Tests, although private so that we can create two versions of each,
    // ======== one for nodes and one for relationships

    private void makeSureFirstAndLastAreReturnedCorrectly( EntityCreator<PropertyContainer> creator,
            TimelineIndex<PropertyContainer> timeline )
    {
        LinkedList<Pair<PropertyContainer, Long>> timestamps = createTimestamps( creator, timeline, 223456, 12345, 432234 );
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( timestamps.get( 1 ).first(), timeline.getFirst() );
            assertEquals( timestamps.getLast().first(), timeline.getLast() );
            tx.success();
        }
    }

    private void makeSureRangesAreReturnedInCorrectOrder( EntityCreator<PropertyContainer> creator,
            TimelineIndex<PropertyContainer> timeline )
    {
        LinkedList<Pair<PropertyContainer, Long>> timestamps = createTimestamps( creator, timeline,
                300000, 200000, 400000, 100000, 500000, 600000, 900000, 800000 );
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( sortedEntities( timestamps, false ),
                    asCollection( timeline.getBetween( null, null ).iterator() ) );
            tx.success();
        }
    }

    private void makeSureRangesAreReturnedInCorrectReversedOrder( EntityCreator<PropertyContainer> creator,
            TimelineIndex<PropertyContainer> timeline )
    {
        LinkedList<Pair<PropertyContainer, Long>> timestamps = createTimestamps( creator, timeline,
                300000, 200000, 199999, 400000, 100000, 500000, 600000, 900000, 800000 );
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( sortedEntities( timestamps, true ),
                    asCollection( timeline.getBetween( null, null, true ).iterator() ) );
            tx.success();
        }
    }

    private void makeSureWeCanQueryLowerDefaultThan1970( EntityCreator<PropertyContainer> creator,
            TimelineIndex<PropertyContainer> timeline )
    {
        LinkedList<Pair<PropertyContainer,Long>> timestamps = createTimestamps( creator, timeline, -10000, 0, 10000 );
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( sortedEntities( timestamps, true ),
                    asCollection( timeline.getBetween( null, 10000L, true ).iterator() ) );
            tx.success();
        }
    }

    private void makeSureUncommittedChangesAreSortedCorrectly( EntityCreator<PropertyContainer> creator,
            TimelineIndex<PropertyContainer> timeline )
    {
        LinkedList<Pair<PropertyContainer, Long>> timestamps = createTimestamps( creator, timeline,
                300000, 100000, 500000, 900000, 800000 );

        try ( Transaction tx = db.beginTx() )
        {
            timestamps.addAll( createTimestamps( creator, timeline, 40000, 70000, 20000 ) );
            assertEquals( sortedEntities( timestamps, false ),
                    asCollection( timeline.getBetween( null, null ).iterator() ) );
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertEquals( sortedEntities( timestamps, false ),
                    asCollection( timeline.getBetween( null, null ).iterator() ) );
        }
    }

    // ======== The tests

    @Test
    public void makeSureFirstAndLastAreReturnedCorrectlyNode()
    {
        makeSureFirstAndLastAreReturnedCorrectly( nodeCreator, nodeTimeline() );
    }

    @Test
    public void makeSureFirstAndLastAreReturnedCorrectlyRelationship()
    {
        makeSureFirstAndLastAreReturnedCorrectly( relationshipCreator, relationshipTimeline() );
    }

    @Test
    public void makeSureRangesAreReturnedInCorrectOrderNode()
    {
        makeSureRangesAreReturnedInCorrectOrder( nodeCreator, nodeTimeline() );
    }

    @Test
    public void makeSureRangesAreReturnedInCorrectOrderRelationship()
    {
        makeSureRangesAreReturnedInCorrectOrder( relationshipCreator, relationshipTimeline() );
    }

    @Test
    public void makeSureRangesAreReturnedInCorrectReversedOrderNode()
    {
        makeSureRangesAreReturnedInCorrectReversedOrder( nodeCreator, nodeTimeline() );
    }

    @Test
    public void makeSureRangesAreReturnedInCorrectReversedOrderRelationship()
    {
        makeSureRangesAreReturnedInCorrectReversedOrder( relationshipCreator, relationshipTimeline() );
    }

    @Test
    public void makeSureUncommittedChangesAreSortedCorrectlyNode()
    {
        makeSureUncommittedChangesAreSortedCorrectly( nodeCreator, nodeTimeline() );
    }

    @Test
    public void makeSureUncommittedChangesAreSortedCorrectlyRelationship()
    {
        makeSureUncommittedChangesAreSortedCorrectly( relationshipCreator, relationshipTimeline() );
    }

    @Test
    public void makeSureWeCanQueryLowerDefaultThan1970Node()
    {
        makeSureWeCanQueryLowerDefaultThan1970( nodeCreator, nodeTimeline() );
    }
    @Test
    public void makeSureWeCanQueryLowerDefaultThan1970Relationship()
    {
        makeSureWeCanQueryLowerDefaultThan1970( relationshipCreator, relationshipTimeline() );
    }
}
