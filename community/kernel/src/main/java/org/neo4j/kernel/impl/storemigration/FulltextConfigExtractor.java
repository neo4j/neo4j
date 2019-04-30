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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * This class is the amber in which 3.5 fulltext index config reading is preserved in.
 * It has the ability to extract index configuration from a 3.5 fulltext index directory.
 */
class FulltextConfigExtractor
{
    private static final String INDEX_CONFIG_FILE = "fulltext-index.properties";
    private static final String INDEX_CONFIG_ANALYZER = "analyzer";
    private static final String INDEX_CONFIG_EVENTUALLY_CONSISTENT = "eventually_consistent";

    static Map<String,Value> indexConfigFromFulltextDirectory( FileSystemAbstraction fs, File fulltextIndexDirectory )
    {
        File settingsFile = new File( fulltextIndexDirectory, INDEX_CONFIG_FILE );
        Properties settings = new Properties();
        if ( fs.fileExists( settingsFile ) )
        {
            try ( Reader reader = fs.openAsReader( settingsFile, StandardCharsets.UTF_8 ) )
            {
                settings.load( reader );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Failed to read persisted fulltext index properties: " + settingsFile, e );
            }
        }
            //todo
            // - Real target index provider needs to be called here so that we use correct name for all config options.
        HashMap<String,Value> indexConfig = new HashMap<>();
        extractSetting( settings, indexConfig, INDEX_CONFIG_ANALYZER );
        extractSetting( settings, indexConfig, INDEX_CONFIG_EVENTUALLY_CONSISTENT );
        System.out.println( "fulltextIndexDirectory = " + fulltextIndexDirectory );
        System.out.println( indexConfig );
        return indexConfig;
    }

    private static void extractSetting( Properties settings, HashMap<String,Value> indexConfig, String setting )
    {
        String property = settings.getProperty( setting );
        if ( property != null )
        {
            indexConfig.put( "fulltext." + setting, Values.stringValue( property ) );
        }
    }
}
