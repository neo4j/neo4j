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
package org.neo4j.kernel.impl.store;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.test.Race;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NeoStoresIT
{
    @ClassRule
    public static final DatabaseRule db = new EmbeddedDatabaseRule()
            .withSetting(  GraphDatabaseSettings.dense_node_threshold, "1");

    private static final RelationshipType FRIEND = RelationshipType.withName( "FRIEND" );

    private static final String LONG_STRING_VALUE =
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALONG!!";

    @Test
    public void shouldWriteOutTheDynamicChainBeforeUpdatingThePropertyRecord()
            throws Throwable
    {
        Race race = new Race();
        long[] latestNodeId = new long[1];
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        long endTime = currentTimeMillis() + SECONDS.toMillis( 2 );
        race.withEndCondition( () -> (writes.get() > 100 && reads.get() > 10_000) || currentTimeMillis() > endTime );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                latestNodeId[0] = node.getId();
                node.setProperty( "largeProperty", LONG_STRING_VALUE );
                tx.success();
            }
            writes.incrementAndGet();
        } );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.getGraphDatabaseAPI().beginTx() )
            {
                Node node = db.getGraphDatabaseAPI().getNodeById( latestNodeId[0] );
                for ( String propertyKey : node.getPropertyKeys() )
                {
                    node.getProperty( propertyKey );
                }
                tx.success();
            }
            catch ( NotFoundException e )
            {
                // This will catch nodes not found (expected) and also PropertyRecords not found (shouldn't happen
                // but handled in shouldWriteOutThePropertyRecordBeforeReferencingItFromANodeRecord)
            }
            reads.incrementAndGet();
        } );
        race.go();
    }

    @Test
    public void shouldWriteOutThePropertyRecordBeforeReferencingItFromANodeRecord()
            throws Throwable
    {
        Race race = new Race();
        long[] latestNodeId = new long[1];
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        long endTime = currentTimeMillis() + SECONDS.toMillis( 2 );
        race.withEndCondition( () -> (writes.get() > 100 && reads.get() > 10_000) || currentTimeMillis() > endTime );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                latestNodeId[0] = node.getId();
                node.setProperty( "largeProperty", LONG_STRING_VALUE );
                tx.success();
            }
            writes.incrementAndGet();
        } );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.getGraphDatabaseAPI().beginTx() )
            {
                Node node = db.getGraphDatabaseAPI().getNodeById( latestNodeId[0] );

                for ( String propertyKey : node.getPropertyKeys() )
                {
                    node.getProperty( propertyKey );
                }
                tx.success();
            }
            catch ( NotFoundException e )
            {
                if ( Exceptions.contains( e, InvalidRecordException.class ) )
                {
                    throw e;
                }
            }
            reads.incrementAndGet();
        } );
        race.go();
    }

    @Test
    public void shouldWriteOutThePropertyRecordBeforeReferencingItFromARelationshipRecord()
            throws Throwable
    {
        final long node1Id;
        final long node2Id;
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1Id = node1.getId();

            Node node2 = db.createNode();
            node2Id = node2.getId();

            tx.success();
        }

        Race race = new Race();
        final long[] latestRelationshipId = new long[1];
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        long endTime = currentTimeMillis() + SECONDS.toMillis( 2 );
        race.withEndCondition( () -> (writes.get() > 100 && reads.get() > 10_000) || currentTimeMillis() > endTime );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node1 = db.getGraphDatabaseAPI().getNodeById( node1Id );
                Node node2 = db.getGraphDatabaseAPI().getNodeById( node2Id );

                Relationship rel = node1.createRelationshipTo( node2, FRIEND );
                latestRelationshipId[0] = rel.getId();
                rel.setProperty( "largeProperty", LONG_STRING_VALUE );

                tx.success();
            }
            writes.incrementAndGet();
        } );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.getGraphDatabaseAPI().beginTx() )
            {
                Relationship rel = db.getGraphDatabaseAPI().getRelationshipById( latestRelationshipId[0] );

                for ( String propertyKey : rel.getPropertyKeys() )
                {
                    rel.getProperty( propertyKey );
                }
                tx.success();
            }
            catch ( NotFoundException e )
            {
                if ( Exceptions.contains( e, InvalidRecordException.class ) )
                {
                    throw e;
                }
            }
            reads.incrementAndGet();
        } );
        race.go();
    }
}
