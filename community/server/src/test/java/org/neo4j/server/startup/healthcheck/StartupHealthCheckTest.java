/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.startup.healthcheck;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;

public class StartupHealthCheckTest
{

    @Test
    public void shouldPassWithNoRules()
    {
        StartupHealthCheck check = new StartupHealthCheck();
        assertTrue( check.run() );
    }

    @Test
    public void shouldRunAllHealthChecksToCompletionIfNonFail()
    {
        StartupHealthCheck check = new StartupHealthCheck( getPassingRules() );
        assertTrue( check.run() );
    }

    @Test
    public void shouldFailIfOneOrMoreHealthChecksFail()
    {
        StartupHealthCheck check = new StartupHealthCheck( getWithOneFailingRule() );
        assertFalse( check.run() );
    }

    @Test
    public void shouldLogFailedRule()
    {
        StartupHealthCheck check = new StartupHealthCheck( getWithOneFailingRule() );
        InMemoryAppender appender = new InMemoryAppender( StartupHealthCheck.log );
        check.run();

        // Previously we tested on "SEVERE: blah blah" but that's a string
        // depending
        // on the regional settings of the OS.
        assertThat( appender.toString(), containsString( ": blah blah" ) );
    }

    @Test
    public void shouldAdvertiseFailedRule()
    {
        StartupHealthCheck check = new StartupHealthCheck( getWithOneFailingRule() );
        check.run();
        assertNotNull( check.failedRule() );
    }

    private StartupHealthCheckRule[] getWithOneFailingRule()
    {
        StartupHealthCheckRule[] rules = new StartupHealthCheckRule[5];

        for ( int i = 0; i < rules.length; i++ )
        {
            rules[i] = new StartupHealthCheckRule()
            {
                @Override
                public boolean execute( Properties properties )
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

        rules[rules.length / 2] = new StartupHealthCheckRule()
        {
            @Override
            public boolean execute( Properties properties )
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

    private StartupHealthCheckRule[] getPassingRules()
    {
        StartupHealthCheckRule[] rules = new StartupHealthCheckRule[5];

        for ( int i = 0; i < rules.length; i++ )
        {
            rules[i] = new StartupHealthCheckRule()
            {
                @Override
                public boolean execute( Properties properties )
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
}
