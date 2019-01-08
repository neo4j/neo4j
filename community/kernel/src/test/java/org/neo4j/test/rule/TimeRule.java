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
package org.neo4j.test.rule;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static java.lang.System.currentTimeMillis;

/**
 * Simple rule for quickly measure which part of a test is the most time consuming. Just insert
 * call to {@link #time()} and any number of points in the test and time since start or last call
 * will be displayed along with line number.
 */
public class TimeRule implements TestRule
{
    private long time;
    private String name;

    @Override
    public Statement apply( Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                time( "start" );
                try
                {
                    base.evaluate();
                }
                finally
                {
                    time( "end" );
                }
            }
        };
    }

    public void time( String name )
    {
        this.name = name;
        if ( time == 0 )
        {
            time = currentTimeMillis();
        }
        else
        {
            long now = currentTimeMillis();
            long diff = now - time;
            System.out.println( (name != null ? name + " " : "" ) + diff );
            time = now;
        }
    }

    public void time()
    {
        time( grabFromStackTrace() );
    }

    private String grabFromStackTrace()
    {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        return stack[3].getMethodName() + ":" + stack[3].getLineNumber();
    }
}
