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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.index.IndexWriterConfig;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.INT;

@ServiceProvider
public class LuceneSettings implements SettingsDeclaration
{
    @Internal
    public static Setting<Integer> lucene_writer_max_buffered_docs =
            newBuilder( "unsupported.dbms.index.lucene.writer_max_buffered_docs", INT, 100000 ).build();

    @Internal
    public static Setting<Integer> lucene_population_max_buffered_docs =
            newBuilder( "unsupported.dbms.index.lucene.population_max_buffered_docs", INT, IndexWriterConfig.DISABLE_AUTO_FLUSH ).build();

    @Internal
    public static Setting<Integer> lucene_merge_factor = newBuilder( "unsupported.dbms.index.lucene.merge_factor", INT, 2 ).build();

    @Internal
    public static Setting<Double> lucene_nocfs_ratio = newBuilder( "unsupported.dbms.index.lucene.nocfs.ratio", DOUBLE, 1.0 ).build();

    @Internal
    public static Setting<Double> lucene_min_merge = newBuilder( "unsupported.dbms.index.lucene.min_merge", DOUBLE, 0.1 ).build();

    @Internal
    public static Setting<Double> lucene_standard_ram_buffer_size =
            newBuilder( "unsupported.dbms.index.lucene.standard_ram_buffer_size", DOUBLE, IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB ).build();

    @Internal
    public static Setting<Double> lucene_population_ram_buffer_size =
            newBuilder( "unsupported.dbms.index.lucene.population_ram_buffer_size", DOUBLE, 50D ).build();
}
