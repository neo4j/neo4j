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
package org.neo4j.server.preflight;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.SuppressOutput;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.test.SuppressOutput.suppressAll;

public class TestPreflightTasks
{
    @Test
    public void shouldPassWithNoRules()
    {
        PreFlightTasks check = new PreFlightTasks( NullLogProvider.getInstance() );
        assertTrue( check.run() );
    }

    @Test
    public void shouldRunAllHealthChecksToCompletionIfNonFail()
    {
        PreFlightTasks check = new PreFlightTasks( NullLogProvider.getInstance(), getPassingRules() );
        assertTrue( check.run() );
    }

    @Test
    public void shouldFailIfOneOrMoreHealthChecksFail()
    {
        PreFlightTasks check = new PreFlightTasks( NullLogProvider.getInstance(), getWithOneFailingRule() );
        assertFalse( check.run() );
    }

    @Test
    public void shouldLogFailedRule()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        PreFlightTasks check = new PreFlightTasks( logProvider, getWithOneFailingRule() );
        check.run();

        logProvider.assertExactly(
                inLog( PreFlightTasks.class ).error( "blah blah" )
        );
    }

    @Test
    public void shouldAdvertiseFailedRule()
    {
        PreFlightTasks check = new PreFlightTasks( NullLogProvider.getInstance(), getWithOneFailingRule() );
        check.run();
        assertNotNull( check.failedTask() );
    }

    private PreflightTask[] getWithOneFailingRule()
    {
        PreflightTask[] rules = new PreflightTask[5];

        for ( int i = 0; i < rules.length; i++ )
        {
            rules[i] = new PreflightTask()
            {
                @Override
                public boolean run()
                {
                    return true;
                }

                @Override
                public String getFailureMessage()
                {
                    return "blah blah";
                }
            };
        }

        rules[rules.length / 2] = new PreflightTask()
        {
            @Override
            public boolean run()
            {
                return false;
            }

            @Override
            public String getFailureMessage()
            {
                return "blah blah";
            }
        };

        return rules;
    }

    private PreflightTask[] getPassingRules()
    {
        PreflightTask[] rules = new PreflightTask[5];

        for ( int i = 0; i < rules.length; i++ )
        {
            rules[i] = new PreflightTask()
            {
                @Override
                public boolean run()
                {
                    return true;
                }

                @Override
                public String getFailureMessage()
                {
                    return "blah blah";
                }
            };
        }

        return rules;
    }

    @Rule
    public SuppressOutput suppressOutput = suppressAll();
}
