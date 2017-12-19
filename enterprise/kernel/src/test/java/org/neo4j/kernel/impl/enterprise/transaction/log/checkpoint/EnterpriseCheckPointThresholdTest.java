/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThresholdTestSupport;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EnterpriseCheckPointThresholdTest extends CheckPointThresholdTestSupport
{
    private boolean haveLogsToPrune;

    @Before
    @Override
    public void setUp()
    {
        super.setUp();
        logPruning = new LogPruning()
        {
            @Override
            public void pruneLogs( long currentVersion )
            {
                fail( "Check point threshold must never call out to prune logs directly." );
            }

            @Override
            public boolean mightHaveLogsToPrune()
            {
                return haveLogsToPrune;
            }
        };
    }

    @Test
    public void checkPointIsNeededIfWeMightHaveLogsToPrune() throws Exception
    {
        withPolicy( "volumetric" );
        haveLogsToPrune = true;
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );
        assertTrue( threshold.isCheckPointingNeeded( 2, triggered ) );
        verifyTriggered( "log pruning" );
        verifyNoMoreTriggers();
    }

    @Test
    public void checkPointIsInitiallyNotNeededIfWeHaveNoLogsToPrune() throws Exception
    {
        withPolicy( "volumetric" );
        haveLogsToPrune = false;
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );
        assertFalse( threshold.isCheckPointingNeeded( 2, notTriggered ) );
        verifyNoMoreTriggers();
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void continuousPolicyMustAlwaysTriggerCheckPoints() throws Exception
    {
        withPolicy( "continuous" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        assertThat( threshold.checkFrequencyMillis(), is( 0L ) );

        assertTrue( threshold.isCheckPointingNeeded( 2, triggered ) );
        threshold.checkPointHappened( 3 );
        assertTrue( threshold.isCheckPointingNeeded( 3, triggered ) );
        assertTrue( threshold.isCheckPointingNeeded( 3, triggered ) );
        verifyTriggered( "continuous" );
        verifyTriggered( "continuous" );
        verifyTriggered( "continuous" );
        verifyNoMoreTriggers();
    }
}
