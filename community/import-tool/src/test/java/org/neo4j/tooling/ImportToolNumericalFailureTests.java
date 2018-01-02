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
package org.neo4j.tooling;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.SuppressOutput;
import org.neo4j.unsafe.impl.batchimport.input.InputException;

import static org.junit.Assert.fail;
import static org.neo4j.tooling.ImportToolTest.assertExceptionContains;
import static org.neo4j.tooling.ImportToolTest.importTool;

/**
 * Tests that we fail correctly when given strings which can't be interpreted as numbers when configured to interpret
 * them as such.
 */
@RunWith( Parameterized.class )
public class ImportToolNumericalFailureTests
{
    @Parameters( name = "{index}: {0}, \"{1}\", \"{2}\"" )
    public static List<Object[]> types()
    {
        ArrayList<Object[]> params = new ArrayList<>();

        for ( String type : Arrays.asList( "int", "long", "short", "byte", "float", "double" ) )
        {
            for ( String val : Arrays.asList( " 1 7 ", " -1 7 ", " - 1 ", "   ", "   -  ", "-", "1. 0", "1 .", ".",
                    "1E 10", " . 1" ) )
            {
                // Only include decimals for floating point
                if ( val.contains( "." ) && !( type.equals( "float" ) || type.equals( "double" ) ) )
                {
                    continue;
                }

                final String error;
                if ( type.equals( "float" ) || type.equals( "double" ) )
                {
                    error = "Not a number: \"" + val + "\"";
                }
                else
                {
                    error = "Not an integer: \"" + val + "\"";
                }

                String[] args = new String[3];
                args[0] = type;
                args[1] = val;
                args[2] = error;

                params.add( args );
            }
        }

        return params;
    }

    @Parameter
    public String type;

    @Parameter( value = 1 )
    public String val;

    @Parameter( value = 2 )
    public String expectedError;

    @Rule
    public final EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( getClass() ).startLazily();
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppress( SuppressOutput.System.values() );

    private int dataIndex;

    @Test
    public void test() throws Exception
    {
        // GIVEN
        File data = file( fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.println( ":LABEL,adult:" + type );
            writer.println( "PERSON," + val );
        }

        try
        {
            // WHEN
            importTool( "--into", dbRule.getStoreDirAbsolutePath(), "--quote", "'", "--nodes", data.getAbsolutePath() );
            // THEN
            fail( "Expected import to fail" );
        }
        catch ( Exception e )
        {
            assertExceptionContains( e, expectedError, InputException.class );
        }
    }

    private String fileName( String name )
    {
        return dataIndex++ + "-" + name;
    }

    private File file( String localname )
    {
        return new File( dbRule.getStoreDir(), localname );
    }
}
