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
package org.neo4j.kernel.impl.locking;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Resource;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.Race;
import org.neo4j.test.extension.ImpermanentDatabaseExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Testing on database level that creating and deleting relationships over the the same two nodes
 * doesn't create unnecessary deadlock scenarios, i.e. that locking order is predictable and symmetrical
 *
 * Also test that relationship chains are consistently read during concurrent updates.
 */
@ExtendWith( {ImpermanentDatabaseExtension.class, RandomExtension.class} )
public class RelationshipCreateDeleteIT
{
    @Resource
    public ImpermanentDatabaseRule db;
    @Resource
    public RandomRule random;

    @Test
    public void shouldNotDeadlockOrCrashFromInconsistency() throws Throwable
    {
        // GIVEN (A) -[R]-> (B)
        final Node a;
        final Node b;
        try ( Transaction tx = db.beginTx() )
        {
            (a = db.createNode()).createRelationshipTo( b = db.createNode(), MyRelTypes.TEST );
            tx.success();
        }

        // WHEN
        Race race = new Race();
        // a bunch of deleters
        for ( int i = 0; i < 30; i++ )
        {
            race.addContestant( () ->
            {
                for ( int j = 0; j < 10; j++ )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        Node node = random.nextBoolean() ? a : b;
                        for ( Relationship relationship : node.getRelationships() )
                        {
                            try
                            {
                                relationship.delete();
                            }
                            catch ( NotFoundException e )
                            {
                                // This is OK and expected since there are multiple threads deleting
                                assertTrue( e.getMessage().contains( "already deleted" ) );
                            }
                        }
                        tx.success();
                    }
                }
            } );
        }

        // a bunch of creators
        for ( int i = 0; i < 30; i++ )
        {
            race.addContestant( () ->
            {
                for ( int j = 0; j < 10; j++ )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        boolean order = random.nextBoolean();
                        Node start = order ? a : b;
                        Node end = order ? b : a;
                        start.createRelationshipTo( end, MyRelTypes.TEST );
                        tx.success();
                    }
                }
            } );
        }

        // THEN there should be no thread throwing exception, especially DeadlockDetectedException
        race.go();
    }
}
