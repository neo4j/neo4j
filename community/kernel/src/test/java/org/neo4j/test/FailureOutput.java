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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Utility for capturing output and printing it on test failure only.
 */
public class FailureOutput implements TestRule
{
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final PrintStream stream = new PrintStream( output );
    private final PrintStream target;

    public FailureOutput()
    {
        this( System.out );
    }

    public FailureOutput( PrintStream target )
    {
        this.target = target;
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    base.evaluate();
                }
                catch ( Throwable failure )
                {
                    printOutput();
                    throw failure;
                }
            }
        };
    }

    private void printOutput() throws IOException
    {
        target.write( output.toByteArray() );
        target.flush();
    }

    public PrintStream stream()
    {
        return stream;
    }

    public PrintWriter writer()
    {
        return new PrintWriter( stream );
    }
}
