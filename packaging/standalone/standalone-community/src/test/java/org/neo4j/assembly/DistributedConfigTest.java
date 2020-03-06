/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.assembly;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class DistributedConfigTest
{
    private static final Pattern SETTING_MATCHER = Pattern.compile( "^(\\w+(\\.\\w+)+)\\s*=(.*)$" );

    @Inject
    TestDirectory testDirectory;

    @Test
    void allSettingsShouldBeValid() throws IOException
    {
        List<String> mentionedSettings = new ArrayList<>();
        Path of = Path.of( "src", "main", "distribution", "text", "community", "conf", "neo4j.conf" );
        List<String> lines = Files.readAllLines( of );
        File newConfig = testDirectory.file( "tmp.conf" );
        boolean multiLine = false;
        try ( PrintWriter writer = new PrintWriter( Files.newBufferedWriter( newConfig.toPath() ) ) )
        {
            for ( String line : lines )
            {
                multiLine = processLine( mentionedSettings, writer, line, multiLine );
            }
        }

        // Throws on errors
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.strict_config_validation, Boolean.TRUE )
                .fromFile( newConfig ).build();

        // Check the settings without values
        Map<String,Setting<Object>> availableSettings = config.getDeclaredSettings();
        for ( String mentionedSetting : mentionedSettings )
        {
            assertTrue( availableSettings.containsKey( mentionedSetting ) );
        }
    }

    private static boolean processLine( List<String> mentionedSettings, PrintWriter writer, String line, boolean multiLine )
    {
        if ( !line.startsWith( "#" ) )
        {
            writer.println( line );
            return false;
        }
        // This is a comment
        String uncommented = line.substring( 1 ).trim();

        Matcher matcher = SETTING_MATCHER.matcher( uncommented );
        if ( matcher.matches() )
        {
            // This is a commented setting
            if ( matcher.group( 3 ).length() == 0 )
            {
                // And we have no default value, we have to probe the config later
                mentionedSettings.add( matcher.group( 1 ) );
            }
            else
            {
                // We have a fully working setting, write it out to the file.
                writer.println( uncommented );
            }
            return uncommented.endsWith( "\\" );
        }
        else if ( multiLine && uncommented.endsWith( "\\" ) )
        {
            writer.println( uncommented );
            return true;
        }
        return false;
    }
}
