/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.Race;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.lang.String.format;

import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class ReadIndexWritesUnderConcurrentLoadStressIT
{
    public static final int TX_COUNT = 16_000;
    public static final int THREAD_COUNT = 30;
    public static final DecimalFormat COUNT_FORMAT = new DecimalFormat( "###,###,###,###,##0" );
    public static final DecimalFormat THROUGHPUT_FORMAT = new DecimalFormat( "###,###,###,###,##0.00" );
    public static final Label LABEL = Label.label( "Label" );
    public static final String PROPERTY_KEY = "key";

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() )
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.pagecache_memory, "2000M" );
            builder.setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "500M" );
        };
    };

    @Test
    public void shouldReadNodeWrittenInPreviousTransaction() throws Throwable
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create();
            tx.success();
        }

        int threadTxCount = TX_COUNT / THREAD_COUNT;
        int endOfRange = -1;
        AtomicLong txs = new AtomicLong( 0 );
        Race race = new Race();
        for ( int t = 0; t < THREAD_COUNT; t++ )
        {
            int startOfRange = 1 + endOfRange;
            endOfRange = startOfRange + threadTxCount - 1;
            int finalEndOfRange = endOfRange;
            race.addContestant( () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = db.createNode( LABEL );
                    node.setProperty( PROPERTY_KEY, startOfRange );
                    tx.success();
                    txs.incrementAndGet();
                }

                for ( int i = startOfRange + 1; i <= finalEndOfRange; i++ )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        Node node = db.createNode( LABEL );
                        node.setProperty( PROPERTY_KEY, i );
                        Node previousNode = db.findNode( LABEL, PROPERTY_KEY, i - 1 );
                        assertThat( format( "Error at tx %s", i ), previousNode, not( nullValue() ) );
                        tx.success();
                        txs.incrementAndGet();
                    }
                }
            } );
        }
        race.go();
    }
}
