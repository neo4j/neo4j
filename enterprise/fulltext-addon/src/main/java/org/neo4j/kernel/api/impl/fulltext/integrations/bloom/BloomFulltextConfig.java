/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.LUCENE_FULLTEXT_ADDON_PREFIX;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.buildSetting;
import static org.neo4j.kernel.configuration.Settings.illegalValueMessage;
import static org.neo4j.kernel.configuration.Settings.matchesAny;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Configuration parameters for the bloom fulltext addon.
 */
public class BloomFulltextConfig implements LoadableConfig
{
    public static final String UNSUPPORTED_PROPERTY_KEY_REGEX = "^(?!" + LUCENE_FULLTEXT_ADDON_PREFIX + ").+$";
    public static final BiFunction<List<String>,Function<String,String>,List<String>> ILLEGAL_VALUE_CONSTRAINT =
            illegalValueMessage( "Must not contain '" + LUCENE_FULLTEXT_ADDON_PREFIX + "'", matchesAny(
                    UNSUPPORTED_PROPERTY_KEY_REGEX ) );

    @Description( "Enable the fulltext addon for bloom." )
    @Internal
    static final Setting<Boolean> bloom_enabled = setting( "unsupported.dbms.bloom_enabled", BOOLEAN, FALSE );

    @Description( "Property keys to index" )
    @Internal
    static final Setting<List<String>> bloom_indexed_properties =
            buildSetting( "unsupported.dbms.bloom_indexed_properties", STRING_LIST, "" )
                    .constraint( ILLEGAL_VALUE_CONSTRAINT ).build();

    @Description( "Define the analyzer to use for the bloom index. Expects the fully qualified classname of the " +
                  "analyzer to use" )
    @Internal
    static final Setting<String> bloom_analyzer = setting( "unsupported.dbms.bloom_analyzer", STRING,
            "org.apache.lucene.analysis.standard.StandardAnalyzer" );
}
