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
package org.neo4j.consistency;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.Race;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.SuppressOutput;

import static java.lang.Integer.max;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.progress.ProgressMonitorFactory.NONE;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.logging.FormattedLogProvider.toOutputStream;

/**
 * This is a test for triggering a race which was found in and around {@link RecordStorageEngine#apply(CommandsToApply, TransactionApplicationMode)}
 * where e.g. a transaction A which did CREATE NODE N and transaction B which did DELETE NODE N would have a chance to be applied to the
 * {@link LabelScanStore} in the reverse order, i.e. transaction B before transaction A, resulting in outdated label data remaining in the label index.
 */
public class LabelScanStoreTxApplyRaceIT
{
    // === CONTROL PANEL ===
    private static final int NUMBER_OF_DELETORS = 2;
    private static final int NUMBER_OF_CREATORS = max( 2, Runtime.getRuntime().availableProcessors() - NUMBER_OF_DELETORS );
    private static final float CHANCE_LARGE_TX = 0.1f;
    private static final float CHANCE_TO_DELETE_BY_SAME_THREAD = 0.9f;
    private static final int LARGE_TX_SIZE = 3_000;

    private static final Label[] LABELS = TestLabels.values();

    @Rule
    public final EmbeddedDatabaseRule db = new EmbeddedDatabaseRule();

    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    /**
     * The test case is basically loads of concurrent CREATE/DELETE NODE or sometimes just CREATE, keeping the created node in an array
     * for dedicated deleter threads to pick up and delete as fast as they can see them. This concurrently with large creation transactions.
     */
    @Test
    public void shouldStressIt() throws Throwable
    {
        // given
        Race race = new Race().withMaxDuration( 5, TimeUnit.SECONDS );
        AtomicReferenceArray<Node> nodeHeads = new AtomicReferenceArray<>( NUMBER_OF_CREATORS );
        for ( int i = 0; i < NUMBER_OF_CREATORS; i++ )
        {
            race.addContestant( creator( nodeHeads, i ) );
        }
        race.addContestants( NUMBER_OF_DELETORS, deleter( nodeHeads ) );

        // when
        race.go();

        // then
        File storeDir = db.getStoreDir();
        db.shutdownAndKeepStore();
        assertTrue( new ConsistencyCheckService().runFullConsistencyCheck( storeDir, defaults(), NONE,
                toOutputStream( System.out ), false, new ConsistencyFlags( true, true, true, false ) ).isSuccessful() );
    }

    private Runnable creator( AtomicReferenceArray<Node> nodeHeads, int guy )
    {
        return new Runnable()
        {
            private final ThreadLocalRandom random = ThreadLocalRandom.current();

            @Override
            public void run()
            {
                if ( random.nextFloat() < CHANCE_LARGE_TX )
                {
                    // Few large transactions
                    try ( Transaction tx = db.beginTx() )
                    {
                        for ( int i = 0; i < LARGE_TX_SIZE; i++ )
                        {
                            // Nodes are created with properties here. Whereas the properties don't have a functional
                            // impact on this test they do affect timings so that the issue is (was) triggered more often
                            // and therefore have a positive effect on this test.
                            db.createNode( randomLabels() ).setProperty( "name", randomUUID().toString() );
                        }
                        tx.success();
                    }
                }
                else
                {
                    // Many small create/delete transactions
                    Node node;
                    try ( Transaction tx = db.beginTx() )
                    {
                        node = db.createNode( randomLabels() );
                        nodeHeads.set( guy, node );
                        tx.success();
                    }
                    if ( random.nextFloat() < CHANCE_TO_DELETE_BY_SAME_THREAD )
                    {
                        // Most of the time delete in this thread
                        if ( nodeHeads.getAndSet( guy, null ) != null )
                        {
                            try ( Transaction tx = db.beginTx() )
                            {
                                node.delete();
                                tx.success();
                            }
                        }
                        // Otherwise there will be other threads sitting there waiting for these nodes and deletes them if they can
                    }
                }
            }

            private Label[] randomLabels()
            {
                Label[] labels = new Label[LABELS.length];
                int cursor = 0;
                for ( int i = 0; i < labels.length; i++ )
                {
                    if ( random.nextBoolean() )
                    {
                        labels[cursor++] = LABELS[i];
                    }
                }
                if ( cursor == 0 )
                {
                    // at least one
                    labels[cursor++] = LABELS[0];
                }
                return Arrays.copyOf( labels, cursor );
            }
        };
    }

    private Runnable deleter( AtomicReferenceArray<Node> nodeHeads )
    {
        return new Runnable()
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            @Override
            public void run()
            {
                int guy = random.nextInt( NUMBER_OF_CREATORS );
                Node node = nodeHeads.getAndSet( guy, null );
                if ( node != null )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        node.delete();
                        tx.success();
                    }
                    catch ( NotFoundException e )
                    {
                        // This is OK in this test
                    }
                }
            }
        };
    }
}
