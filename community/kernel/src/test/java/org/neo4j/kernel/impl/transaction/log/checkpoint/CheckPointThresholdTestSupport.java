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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Before;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CheckPointThresholdTestSupport
{
    protected Config config;
    protected FakeClock clock;
    protected LogPruning logPruning;
    protected LogProvider logProvider;
    protected Integer intervalTx;
    protected Duration intervalTime;
    protected Consumer<String> notTriggered;
    protected BlockingQueue<String> triggerConsumer;
    protected Consumer<String> triggered;

    @Before
    public void setUp()
    {
        config = Config.defaults();
        clock = Clocks.fakeClock();
        logPruning = LogPruning.NO_PRUNING;
        logProvider = NullLogProvider.getInstance();
        intervalTx = config.get( GraphDatabaseSettings.check_point_interval_tx );
        intervalTime = config.get( GraphDatabaseSettings.check_point_interval_time );
        triggerConsumer = new LinkedBlockingQueue<>();
        triggered = triggerConsumer::offer;
        notTriggered = s -> fail( "Should not have triggered: " + s );
    }

    protected void withPolicy( String policy )
    {
        config.augment( stringMap( GraphDatabaseSettings.check_point_policy.name(), policy ) );
    }

    protected void withIntervalTime( String time )
    {
        config.augment( stringMap( GraphDatabaseSettings.check_point_interval_time.name(), time ) );
    }

    protected void withIntervalTx( int count )
    {
        config.augment( stringMap( GraphDatabaseSettings.check_point_interval_tx.name(), String.valueOf( count ) ) );
    }

    protected CheckPointThreshold createThreshold()
    {
        return CheckPointThreshold.createThreshold( config, clock, logPruning, logProvider );
    }

    protected void verifyTriggered( String reason )
    {
        assertThat( triggerConsumer.poll(), containsString( reason ) );
    }

    protected void verifyNoMoreTriggers()
    {
        assertTrue( triggerConsumer.isEmpty() );
    }
}
