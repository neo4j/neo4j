/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration;

import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.STRING;

import java.time.Duration;
import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.impl.fulltext.analyzer.providers.StandardNoStopWords;

@ServiceProvider
@PublicApi
public class FulltextSettings implements SettingsDeclaration {
    private static final String DEFAULT_ANALYZER = StandardNoStopWords.ANALYZER_NAME;

    @Description("The name of the analyzer that the fulltext indexes should use by default.")
    public static final Setting<String> fulltext_default_analyzer = newBuilder(
                    "db.index.fulltext.default_analyzer", STRING, DEFAULT_ANALYZER)
            .build();

    @Description("Whether or not fulltext indexes should be eventually consistent by default or not.")
    public static final Setting<Boolean> eventually_consistent =
            newBuilder("db.index.fulltext.eventually_consistent", BOOL, false).build();

    @Description(
            "The number of threads that are applying the queued index updates for eventually consistent fulltext indexes")
    public static final Setting<Integer> eventually_consistent_apply_parallelism = newBuilder(
                    "db.index.fulltext.eventually_consistent_apply_parallelism", INT, 1)
            .addConstraint(min(1))
            .build();

    @Description(
            "How often an eventually consistent fulltext index will be refreshed (readers guaranteed to see changes). "
                    + "If set to 0 then refresh is done by the threads applying eventually consistent fulltext index updates")
    public static final Setting<Duration> eventually_consistent_refresh_interval = newBuilder(
                    "db.index.fulltext.eventually_consistent_refresh_interval", DURATION, Duration.ofSeconds(0))
            .build();

    @Description("The number of threads that can do fulltext index refresh in parallel, "
            + "basically how many eventually consistent fulltext indexes can be refreshed in parallel")
    public static final Setting<Integer> eventually_consistent_refresh_parallelism = newBuilder(
                    "db.index.fulltext.eventually_consistent_refresh_parallelism", INT, 1)
            .addConstraint(min(1))
            .build();

    @Description(
            "The eventually_consistent mode of the fulltext indexes works by queueing up index updates to be applied later in a background thread. "
                    + "This newBuilder sets an upper bound on how many index updates are allowed to be in this queue at any one point in time. When it is reached, "
                    + "the commit process will slow down and wait for the index update applier thread to make some more room in the queue.")
    public static final Setting<Integer> eventually_consistent_index_update_queue_max_length = newBuilder(
                    "db.index.fulltext.eventually_consistent_index_update_queue_max_length", INT, 10000)
            .addConstraint(range(1, 50_000_000))
            .build();
}
