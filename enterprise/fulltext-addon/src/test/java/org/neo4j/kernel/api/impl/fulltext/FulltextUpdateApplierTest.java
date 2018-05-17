/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.concurrent.ExecutionException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.JobScheduler;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class FulltextUpdateApplierTest
{
    private LifeSupport life;
    private FulltextUpdateApplier applier;
    private AvailabilityGuard availabilityGuard;
    private JobScheduler scheduler;
    private Log log;

    @Before
    public void setUp()
    {
        life = new LifeSupport();
        log = NullLog.getInstance();
        availabilityGuard = new AvailabilityGuard( Clock.systemUTC(), log );
        scheduler = life.add( new CentralJobScheduler() );
        life.start();
    }

    private void startApplier()
    {
        applier = life.add( new FulltextUpdateApplier( log, availabilityGuard, scheduler ) );
    }

    @After
    public void tearDown()
    {
        life.shutdown();
    }

    @Test
    public void exceptionsDuringIndexUpdateMustPropagateToTheCaller() throws Exception
    {
        startApplier();
        AsyncFulltextIndexOperation op = applier.updatePropertyData( null, null );

        try
        {
            op.awaitCompletion();
            fail( "awaitCompletion should have thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), is( instanceOf( NullPointerException.class ) ) );
        }
    }

    @Test
    public void exceptionsDuringNodePopulationMustBeLoggedAndMarkTheIndexAsFailed() throws Exception
    {
        startApplier();
        LuceneFulltext index = new StubLuceneFulltext();
        GraphDatabaseService db = new StubGraphDatabaseService();
        WritableFulltext writableFulltext = new WritableFulltext( index );
        AsyncFulltextIndexOperation op = applier.populateNodes( writableFulltext, db );

        try
        {
            op.awaitCompletion();
            fail( "awaitCompletion should have thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), is( instanceOf( NullPointerException.class ) ) );
        }
        assertThat( index.getState(), is( InternalIndexState.FAILED ) );
    }

    @Test
    public void exceptionsDuringRelationshipPopulationMustBeLoggedAndMarkTheIndexAsFailed() throws Exception
    {
        startApplier();
        LuceneFulltext index = new StubLuceneFulltext();
        GraphDatabaseService db = new StubGraphDatabaseService();
        WritableFulltext writableFulltext = new WritableFulltext( index );
        AsyncFulltextIndexOperation op = applier.populateRelationships( writableFulltext, db );

        try
        {
            op.awaitCompletion();
            fail( "awaitCompletion should have thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), is( instanceOf( NullPointerException.class ) ) );
        }
        assertThat( index.getState(), is( InternalIndexState.FAILED ) );
    }
}
