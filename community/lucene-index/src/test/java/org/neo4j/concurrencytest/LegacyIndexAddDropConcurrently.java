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
package org.neo4j.concurrencytest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.ImpermanentDatabaseRule;

public class LegacyIndexAddDropConcurrently
{
    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @Test
    public void shouldHandleConcurrentIndexDropping() throws Exception
    {
        // Given
        ExecutorService exec = Executors.newFixedThreadPool( 4 );
        final GraphDatabaseService db = dbRule.getGraphDatabaseService();

        List<Callable<Object>> jobs = new ArrayList<>();
        for ( int i = 0; i < 4; i++ )
        {
            jobs.add( new Callable<Object>()
            {
                private final Random rand = ThreadLocalRandom.current();

                @Override
                public Object call() throws Exception
                {
                    for ( int j = 0; j < 1000; j++ )
                    {
                        switch ( rand.nextInt( 5 ) )
                        {
                            case 4:
                                // 1 in 5 chance, drop the index
                                try ( Transaction tx = db.beginTx() )
                                {
                                    db.index().forNodes( "users" ).delete();
                                    tx.success();
                                }
                                catch ( NotFoundException e )
                                {
                                    // Occasionally expected
                                }
                                break;
                            default:
                                // Otherwise, write to it
                                try ( Transaction tx = db.beginTx() )
                                {
                                    db.index().forNodes( "users" ).add( db.createNode(), "name", "steve" );
                                    tx.success();
                                }
                                catch ( NotFoundException e )
                                {
                                    // Occasionally expected
                                }
                                break;
                        }
                    }
                    return null;
                }
            } );
        }

        // When
        for ( Future<Object> objectFuture : exec.invokeAll( jobs ) )
        {
            objectFuture.get();
        }

        // Then no errors should have occurred.
    }
}
