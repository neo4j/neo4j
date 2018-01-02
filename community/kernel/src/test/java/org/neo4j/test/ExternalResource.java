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

/**
 * A better version of {@link org.junit.rules.ExternalResource} that properly handles exceptions in {@link #after()}.
 */
public abstract class ExternalResource implements TestRule
{
    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                before();
                Throwable failure = null;
                try
                {
                    base.evaluate();
                }
                catch ( Throwable e )
                {
                    failure = e;
                }
                finally
                {
                    try
                    {
                        after( failure == null );
                    }
                    catch ( Throwable e )
                    {
                        if ( failure != null )
                        {
                            failure.addSuppressed( e );
                        }
                        else
                        {
                            failure = e;
                        }
                    }
                }
                if ( failure != null )
                {
                    throw failure;
                }
            }
        };
    }

    /**
     * Override to set up your specific external resource.
     *
     * @throws Throwable if setup fails (which will disable {@code after}
     */
    protected void before() throws Throwable
    {
        // do nothing
    }

    /**
     * Override to tear down your specific external resource.
     */
    protected void after( boolean successful ) throws Throwable
    {
        // do nothing
    }
}
