/*
 * Copyright (c) 2002-2019 "Neo Technology,"
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
package org.neo4j.test.rule;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.PrintStream;

public class SamplingProfilerRule implements TestRule, Profiler
{
    private final Profiler profiler = new SamplingProfiler();

    private void beforeEach()
    {
        profiler.reset();
    }

    private void afterEach( boolean failed, Description testDescription ) throws InterruptedException
    {
        profiler.finish();
        if ( failed )
        {
            profiler.printProfile( System.out, "Profile for " + testDescription );
        }
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                beforeEach();
                try
                {
                    base.evaluate();
                    afterEach( false, description );
                }
                catch ( Throwable th )
                {
                    afterEach( true, description );
                    throw th;
                }
            }
        };
    }

    @Override
    public void reset()
    {
        profiler.reset();
    }

    @Override
    public void setSampleIntervalNanos( long nanos )
    {
        profiler.setSampleIntervalNanos( nanos );
    }

    @Override
    public void finish() throws InterruptedException
    {
        profiler.finish();
    }

    @Override
    public void printProfile( PrintStream out, String profileTitle )
    {
        profiler.printProfile( out, profileTitle );
    }

    @Override
    public ProfiledInterval profile( Thread threadToProfile, long initialDelayNanos )
    {
        return profiler.profile( threadToProfile, initialDelayNanos );
    }
}
