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
package org.neo4j.importer;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.importer.ImportCommandTest.assertExceptionContains;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class ImportNumericalFailureTest
{
    @Inject
    private TestDirectory testDirectory;

    static List<String[]> parameters()
    {
        ArrayList<String[]> params = new ArrayList<>();

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

    @ParameterizedTest
    @MethodSource( value = "parameters" )
    void failImportOnInvalidData( String type, String val, String expectedError ) throws Exception
    {
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        File data = file( databaseLayout, fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.println( ":LABEL,adult:" + type );
            writer.println( "PERSON," + val );
        }

        Exception exception = assertThrows( Exception.class,
                () -> runImport( databaseLayout.databaseDirectory().toPath().toAbsolutePath(), "--quote", "'", "--nodes", data.getAbsolutePath() ) );
        assertExceptionContains( exception, expectedError, InputException.class );
    }

    private String fileName( String name )
    {
        return name;
    }

    private File file( DatabaseLayout databaseLayout, String localname )
    {
        return databaseLayout.file( localname );
    }

    private static void runImport( Path homeDir, String... arguments )
    {
        final var ctx = new ExecutionContext( homeDir, homeDir.resolve( "conf" ) );
        final var cmd = new ImportCommand( ctx );
        CommandLine.populateCommand( cmd, arguments );
        cmd.execute();
    }
}
