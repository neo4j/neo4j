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
package org.neo4j.test.randomized;

import java.io.PrintStream;
import java.util.List;

import org.neo4j.test.randomized.RandomizedTester.TargetFactory;

public class TestCaseWriter<T,F>
{
    private final String testName;
    private final Printable given;
    private final List<Action<T,F>> actions;
    private final Action<T,F> failingAction;
    private final TargetFactory<T> targetFactory;

    TestCaseWriter( String testName, Printable given, TargetFactory<T> targetFactory,
            List<Action<T,F>> actions, Action<T,F> failingAction )
    {
        this.testName = testName;
        this.given = given;
        this.targetFactory = targetFactory;
        this.actions = actions;
        this.failingAction = failingAction;
    }

    public void print( PrintStream out )
    {
        T target = targetFactory.newInstance();
        LinePrinter baseLinePrinter = new PrintStreamLinePrinter( out, 0 );
        baseLinePrinter.println( "@Test" );
        baseLinePrinter.println( "public void " + testName + "() throws Exception" );
        baseLinePrinter.println( "{" );

        LinePrinter codePrinter = baseLinePrinter.indent();
        codePrinter.println( "// GIVEN" );
        given.print( codePrinter );
        for ( Action<T,F> action : actions )
        {
            action.printAsCode( target, codePrinter, false );
            action.apply( target );
        }

        codePrinter.println( "" );
        codePrinter.println( "// WHEN/THEN" );
        failingAction.printAsCode( target, codePrinter, true );
        baseLinePrinter.println( "}" );
        out.flush();
    }
}
