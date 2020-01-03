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
package org.neo4j.kernel.api.impl.fulltext;

import org.neo4j.kernel.api.impl.fulltext.analyzer.providers.Standard;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Settings;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.buildSetting;
import static org.neo4j.kernel.configuration.Settings.max;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Configuration settings for the fulltext index.
 */
public class FulltextConfig implements LoadableConfig
{
    private static final String DEFAULT_ANALYZER = Standard.STANDARD_ANALYZER_NAME;

    @Description( "The name of the analyzer that the fulltext indexes should use by default." )
    public static final Setting<String> fulltext_default_analyzer = setting( "dbms.index.fulltext.default_analyzer", STRING, DEFAULT_ANALYZER );

    @Description( "Whether or not fulltext indexes should be eventually consistent by default or not." )
    public static final Setting<Boolean> eventually_consistent = setting( "dbms.index.fulltext.eventually_consistent", BOOLEAN, Settings.FALSE );

    @Description( "The eventually_consistent mode of the fulltext indexes works by queueing up index updates to be applied later in a background thread. " +
                  "This setting sets an upper bound on how many index updates are allowed to be in this queue at any one point in time. When it is reached, " +
                  "the commit process will slow down and wait for the index update applier thread to make some more room in the queue." )
    public static final Setting<Integer> eventually_consistent_index_update_queue_max_length =
            buildSetting( "dbms.index.fulltext.eventually_consistent_index_update_queue_max_length", INTEGER, "10000" )
                    .constraint( min( 1 ) )
                    .constraint( max( 50_000_000 ) )
                    .build();
}
