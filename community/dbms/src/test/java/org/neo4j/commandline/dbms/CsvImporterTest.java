/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.commandline.dbms;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertTrue;

public class CsvImporterTest
{
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory();

    @Test
    public void writesReportToSpecifiedReportFile() throws Exception
    {
        File reportLocation = testDir.file( "the_report" );

        File inputFile = testDir.file( "foobar.csv" );
        List<String> lines = Arrays.asList( "foo,bar,baz" );
        Files.write( inputFile.toPath(), lines, Charset.defaultCharset() );

        CsvImporter csvImporter = new CsvImporter(
                Args.parse( String.format( "--report-file=%s", reportLocation.getAbsolutePath() ),
                        String.format( "--nodes=%s", inputFile.getAbsolutePath() ) ), Config.defaults(),
                new RealOutsideWorld() );

        csvImporter.doImport();

        assertTrue( reportLocation.exists() );
    }
}
