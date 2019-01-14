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
package org.neo4j.test.rule.system;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SystemExitRule extends ExternalResource
{
    private Integer expectedExitStatusCode;
    private SecurityManager originalSecurityManager;

    private SystemExitRule()
    {
    }

    public static SystemExitRule none()
    {
        return new SystemExitRule();
    }

    public void expectExit( int statusCode )
    {
        this.expectedExitStatusCode = statusCode;
    }

    @Override
    protected void before()
    {
        originalSecurityManager = System.getSecurityManager();
        TestSecurityManager testSecurityManager = new TestSecurityManager( originalSecurityManager );
        System.setSecurityManager( testSecurityManager );
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        final Statement externalRuleStatement = super.apply( base, description );
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    externalRuleStatement.evaluate();
                    if ( exitWasExpected() )
                    {
                        fail( "System exit call was expected, but not invoked." );
                    }
                }
                catch ( SystemExitError e )
                {
                    int exceptionStatusCode = e.getStatusCode();
                    if ( exitWasExpected() )
                    {
                        int expectedCode = expectedExitStatusCode;
                        assertEquals( String.format( "Expected system exit code:%d but was: %d.",
                                expectedCode, exceptionStatusCode ), expectedCode, exceptionStatusCode );
                    }
                    else
                    {
                        fail( "System exit call was not expected, but was invoked. Exit status code: " +
                              exceptionStatusCode );
                    }

                }
            }
        };
    }

    @Override
    protected void after()
    {
        System.setSecurityManager( originalSecurityManager );
    }

    private boolean exitWasExpected()
    {
        return expectedExitStatusCode != null;
    }
}
