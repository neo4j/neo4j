/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class is the amber in which 3.5 fulltext index config reading is preserved in.
 * It has the ability to extract index configuration from a 3.5 fulltext index directory.
 */
class FulltextConfigExtractor
{
    private static final String INDEX_CONFIG_FILE = "fulltext-index.properties";
    private static final String INDEX_CONFIG_ANALYZER = "analyzer";
    private static final String INDEX_CONFIG_EVENTUALLY_CONSISTENT = "eventually_consistent";

    static IndexConfig indexConfigFromFulltextDirectory( FileSystemAbstraction fs, Path fulltextIndexDirectory )
    {
        Path settingsFile = fulltextIndexDirectory.resolve( INDEX_CONFIG_FILE );
        Properties settings = new Properties();
        if ( fs.fileExists( settingsFile ) )
        {
            try ( Reader reader = fs.openAsReader( settingsFile, UTF_8 ) )
            {
                settings.load( reader );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Failed to read persisted fulltext index properties: " + settingsFile, e );
            }
        }

        Map<String,Value> indexConfig = new HashMap<>();
        TextValue analyser = extractSetting( settings, INDEX_CONFIG_ANALYZER );
        BooleanValue eventuallyConsistent = extractBooleanSetting( settings, INDEX_CONFIG_EVENTUALLY_CONSISTENT );
        if ( analyser != null )
        {
            indexConfig.put( FulltextIndexSettingsKeys.ANALYZER, analyser );
        }
        if ( eventuallyConsistent != null )
        {
            indexConfig.put( FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT, eventuallyConsistent );
        }
        return IndexConfig.with( indexConfig );
    }

    private static TextValue extractSetting( Properties settings, String setting )
    {
        String property = settings.getProperty( setting );
        if ( property != null )
        {
            return Values.stringValue( property );
        }
        return null;
    }

    private static BooleanValue extractBooleanSetting( Properties settings, String setting )
    {
        String property = settings.getProperty( setting );
        if ( property != null )
        {
            return Values.booleanValue( Boolean.parseBoolean( property ) );
        }
        return null;
    }
}
