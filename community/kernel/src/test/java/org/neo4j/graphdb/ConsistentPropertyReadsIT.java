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
package org.neo4j.graphdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Resource;

import org.neo4j.test.Race;
import org.neo4j.test.extension.EmbeddedDatabaseExtension;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.helpers.ArrayUtil.contains;

/**
 * Test for how properties are read and that they should be read consistently, i.e. adhere to neo4j's
 * interpretation of the ACID guarantees.
 */
@ExtendWith( EmbeddedDatabaseExtension.class )
public class ConsistentPropertyReadsIT
{
    @Resource
    public EmbeddedDatabaseRule db;

    @Test
    public void shouldReadConsistentPropertyValues() throws Throwable
    {
        // GIVEN
        final Node[] nodes = new Node[10];
        final String[] keys = new String[] {"1", "2", "3"};
        final String[] values = new String[] {
                longString( 'a' ),
                longString( 'b' ),
                longString( 'c' ),
        };
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodes.length; i++ )
            {
                nodes[i] = db.createNode();
                for ( String key : keys )
                {
                    nodes[i].setProperty( key, values[0] );
                }
            }
            tx.success();
        }

        int updaters = 10;
        final AtomicLong updatersDone = new AtomicLong( updaters );
        Race race = new Race();
        for ( int i = 0; i < updaters; i++ )
        {
            // Changers
            race.addContestant( () ->
            {
                try
                {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    for ( int j = 0; j < 100; j++ )
                    {
                        Node node = nodes[random.nextInt( nodes.length )];
                        String key = keys[random.nextInt( keys.length )];
                        try ( Transaction tx = db.beginTx() )
                        {
                            node.removeProperty( key );
                            tx.success();
                        }
                        try ( Transaction tx = db.beginTx() )
                        {
                            node.setProperty( key, values[random.nextInt( values.length )] );
                            tx.success();
                        }
                    }
                }
                finally
                {
                    updatersDone.decrementAndGet();
                }
            } );
        }
        for ( int i = 0; i < 100; i++ )
        {
            // Readers
            race.addContestant( () ->
            {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while ( updatersDone.get() > 0 )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        String value = (String) nodes[random.nextInt( nodes.length )]
                                .getProperty( keys[random.nextInt( keys.length )], null );
                        assertTrue( value == null || contains( values, value ), value );
                        tx.success();
                    }
                }
            } );
        }

        // WHEN
        race.go();
    }

    private String longString( char c )
    {
        char[] chars = new char[ThreadLocalRandom.current().nextInt( 800, 1000 )];
        Arrays.fill( chars, c );
        return new String( chars );
    }
}
