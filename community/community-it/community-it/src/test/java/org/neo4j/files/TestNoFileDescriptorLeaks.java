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
package org.neo4j.files;

import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

@DbmsExtension
class TestNoFileDescriptorLeaks
{
    private static final AtomicInteger counter = new AtomicInteger();

    @Inject
    private GraphDatabaseService db;

    @BeforeAll
    static void beforeClass()
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );
        assumeTrue( OsBeanUtil.getOpenFileDescriptors() != OsBeanUtil.VALUE_UNAVAILABLE );
    }

    @Test
    void mustNotLeakFileDescriptorsFromMerge()
    {
        // GIVEN
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "create constraint on (n:Node) assert n.id is unique" );
            tx.commit();
        }
        cycleMerge( 1 );

        long initialFDs = OsBeanUtil.getOpenFileDescriptors();

        // WHEN
        cycleMerge( 300 );

        // THEN
        long finalFDs = OsBeanUtil.getOpenFileDescriptors();
        long upperBoundFDs = initialFDs + 50; // allow some slack
        assertThat( finalFDs, lessThan( upperBoundFDs ) );
    }

    private void cycleMerge( int iterations )
    {
        for ( int i = 0; i < iterations; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.execute(
                        "MERGE (a:Node {id: $a}) " +
                        "MERGE (b:Node {id: $b}) " +
                        "MERGE (c:Node {id: $c}) " +
                        "MERGE (d:Node {id: $d}) " +
                        "MERGE (e:Node {id: $e}) " +
                        "MERGE (f:Node {id: $f}) ",
                        map( "a", nextId() % 100,
                                "b", nextId() % 100,
                                "c", nextId() % 100,
                                "d", nextId(),
                                "e", nextId(),
                                "f", nextId() )
                );
                db.execute( "MERGE (n:Node {id: $a}) ", map( "a", nextId() % 100 ) );
                db.execute( "MERGE (n:Node {id: $a}) ", map( "a", nextId() % 100 ) );
                db.execute( "MERGE (n:Node {id: $a}) ", map( "a", nextId() % 100 ) );
                db.execute( "MERGE (n:Node {id: $a}) ", map( "a", nextId() ) );
                db.execute( "MERGE (n:Node {id: $a}) ", map( "a", nextId() ) );
                db.execute( "MERGE (n:Node {id: $a}) ", map( "a", nextId() ) );
                tx.commit();
            }
        }
    }

    private static int nextId()
    {
        return counter.incrementAndGet();
    }
}
