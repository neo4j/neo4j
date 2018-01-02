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
package files;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.util.OsBeanUtil;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;

public class TestNoFileDescriptorLeaks
{
    private static final AtomicInteger counter = new AtomicInteger();

    @Rule
    public EmbeddedDatabaseRule db = new EmbeddedDatabaseRule( TestNoFileDescriptorLeaks.class );

    @BeforeClass
    public static void beforeClass()
    {
        Assume.assumeFalse( SystemUtils.IS_OS_WINDOWS );
        Assume.assumeThat( OsBeanUtil.getOpenFileDescriptors(), not( OsBeanUtil.VALUE_UNAVAILABLE ) );
    }

    @Test
    public void mustNotLeakFileDescriptorsFromMerge() throws Exception
    {
        // GIVEN
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "create constraint on (n:Node) assert n.id is unique" );
            tx.success();
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
                        "MERGE (a:Node {id: {a}}) " +
                        "MERGE (b:Node {id: {b}}) " +
                        "MERGE (c:Node {id: {c}}) " +
                        "MERGE (d:Node {id: {d}}) " +
                        "MERGE (e:Node {id: {e}}) " +
                        "MERGE (f:Node {id: {f}}) ",
                        map( "a", nextId() % 100,
                                "b", nextId() % 100,
                                "c", nextId() % 100,
                                "d", nextId(),
                                "e", nextId(),
                                "f", nextId() )
                );
                db.execute( "MERGE (n:Node {id: {a}}) ", map( "a", nextId() % 100 ) );
                db.execute( "MERGE (n:Node {id: {a}}) ", map( "a", nextId() % 100 ) );
                db.execute( "MERGE (n:Node {id: {a}}) ", map( "a", nextId() % 100 ) );
                db.execute( "MERGE (n:Node {id: {a}}) ", map( "a", nextId() ) );
                db.execute( "MERGE (n:Node {id: {a}}) ", map( "a", nextId() ) );
                db.execute( "MERGE (n:Node {id: {a}}) ", map( "a", nextId() ) );
                tx.success();
            }
        }
    }

    private static int nextId()
    {
        return counter.incrementAndGet();
    }
}
