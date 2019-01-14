/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThresholdTestSupport;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
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
    public void checkPointIsNeededIfWeMightHaveLogsToPrune()
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
    public void checkPointIsInitiallyNotNeededIfWeHaveNoLogsToPrune()
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
    public void continuousPolicyMustTriggerCheckPointsAfterAnyWriteTransaction()
    {
        withPolicy( "continuous" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        assertThat( threshold.checkFrequencyMillis(), lessThan( CheckPointThreshold.DEFAULT_CHECKING_FREQUENCY_MILLIS ) );

        assertFalse( threshold.isCheckPointingNeeded( 2, triggered ) );
        threshold.checkPointHappened( 3 );
        assertFalse( threshold.isCheckPointingNeeded( 3, triggered ) );
        assertTrue( threshold.isCheckPointingNeeded( 4, triggered ) );
        verifyTriggered( "continuous" );
        verifyNoMoreTriggers();
    }
}
