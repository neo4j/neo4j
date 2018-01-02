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
package org.neo4j.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Set a test to loop a number of times. If you find yourself using this in a production test, you are probably doing
 * something wrong.
 *
 * However, as a temporary measure used locally, it serves as an excellent tool to trigger errors in flaky tests.
 *
 * If used together with other TestRules, usually you need to make sure that this is placed as the LAST field in the
 * test class. This is to ensure that all other rules run inside of this rule, for example to start/stop databases and other resources.
 */
public class RepeatRule implements TestRule
{
    @Retention( RetentionPolicy.RUNTIME )
    @Target(ElementType.METHOD)
    public @interface Repeat
    {
        int times();
    }

    private int count;

    private class RepeatStatement extends Statement
    {
        private final int times;
        private final Statement statement;

        private RepeatStatement( int times, Statement statement )
        {
            this.times = times;
            this.statement = statement;
        }

        @Override
        public void evaluate() throws Throwable
        {
            for ( count = 0; count < times; count++ )
            {
                statement.evaluate();
            }
        }
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        Repeat repeat = description.getAnnotation(Repeat.class);
        if(repeat != null)
        {
            return new RepeatStatement( repeat.times(), base );
        }
        return base;
    }

    /**
     * Get the current count. This can be used (for example) in a non-suspending breakpoint at the beginning of a
     * test to print out what iteration is currently running.
     *
     * @return current count
     */
    public int getCount()
    {
        return count;
    }
}
