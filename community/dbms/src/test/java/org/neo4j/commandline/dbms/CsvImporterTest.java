/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Map;

import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CsvImporterTest
{
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void writesReportToSpecifiedReportFile() throws Exception
    {
        File dbDir = testDir.directory( "db" );
        File logDir = testDir.directory( "logs" );
        File reportLocation = testDir.file( "the_report" );

        File inputFile = testDir.file( "foobar.csv" );
        List<String> lines = Arrays.asList( "foo,bar,baz" );
        Files.write( inputFile.toPath(), lines, Charset.defaultCharset() );

        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld() )
        {
            Config config = Config.defaults()
                .with( additionalConfig() )
                .with( stringMap(
                                DatabaseManagementSystemSettings.database_path.name(), dbDir.getAbsolutePath(),
                                GraphDatabaseSettings.logs_directory.name(), logDir.getAbsolutePath() ) );
            CsvImporter csvImporter = new CsvImporter(
                    Args.parse( String.format( "--report-file=%s", reportLocation.getAbsolutePath() ),
                            String.format( "--nodes=%s", inputFile.getAbsolutePath() ) ), config,
                    outsideWorld );
            csvImporter.doImport();
        }

        assertTrue( reportLocation.exists() );
    }

    private Map<String,String> additionalConfig()
    {
        return stringMap( DatabaseManagementSystemSettings.database_path.name(), getDatabasePath(),
                GraphDatabaseSettings.logs_directory.name(), getLogsDirectory() );
    }

    private String getDatabasePath()
    {
        return testDir.graphDbDir().getAbsolutePath();
    }

    private String getLogsDirectory()
    {
        return testDir.directory( "logs" ).getAbsolutePath();
    }
}
