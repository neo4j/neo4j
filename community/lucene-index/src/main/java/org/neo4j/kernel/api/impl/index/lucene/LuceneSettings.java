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
package org.neo4j.kernel.api.impl.index.lucene;

import static java.lang.Boolean.TRUE;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig.MergePolicyOption.LOG_BYTE_SIZED;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig.MergePolicyOption;

@ServiceProvider
public class LuceneSettings implements SettingsDeclaration {
    @Internal
    @Description("Configure lucene partition size. This is mainly used to test partitioning behaviour without having to"
            + " create Integer.MAX_VALUE indexed entities.")
    public static final Setting<Integer> lucene_max_partition_size =
            newBuilder("internal.dbms.lucene.max_partition_size", INT, null).build();

    @Internal
    @Description("Determines the minimal number of documents required before the buffered in-memory documents are "
            + "flushed as a new Segment. Large values generally give faster indexing.")
    public static final Setting<Integer> lucene_writer_max_buffered_docs = newBuilder(
                    "internal.dbms.index.lucene.writer_max_buffered_docs", INT, 100000)
            .build();

    @Internal
    @Description("Determines the minimal number of documents required before the buffered in-memory documents are "
            + "flushed as a new Segment during population. Large values generally give faster indexing.")
    public static final Setting<Integer> lucene_population_max_buffered_docs = newBuilder(
                    "internal.dbms.index.lucene.population_max_buffered_docs",
                    INT,
                    // Pass in DISABLE_AUTO_FLUSH to prevent triggering a flush due to number of buffered documents
                    LuceneIndexWriterConfig.DISABLE_AUTO_FLUSH)
            .build();

    @Internal
    @Description("Determines how often segment indices are merged by addDocument(). With smaller values, "
            + "less RAM is used while indexing, and searches are faster, but indexing speed is slower. "
            + "With larger values, more RAM is used during indexing, and while searches is slower, indexing is faster.")
    public static final Setting<Integer> lucene_merge_factor =
            newBuilder("internal.dbms.index.lucene.merge_factor", INT, 2).build();

    @Internal
    @Description("Determines how often segment indices are merged by addDocument(). With smaller values, "
            + "less RAM is used while indexing, and searches are faster, but indexing speed is slower. "
            + "With larger values, more RAM is used during indexing, and while searches is slower, indexing is faster. "
            + "This is only used on vector indexes after they have been created.")
    public static final Setting<Integer> vector_standard_merge_factor = newBuilder(
                    "internal.dbms.index.vector.standard_merge_factor", INT, 50)
            .build();

    @Internal
    @Description("Determines how often segment indices are merged by addDocument(). With smaller values, "
            + "less RAM is used while indexing, and searches are faster, but indexing speed is slower. "
            + "With larger values, more RAM is used during indexing, and while searches is slower, indexing is faster. "
            + "This is only used on vector indexes during initial creation.")
    public static final Setting<Integer> vector_population_merge_factor = newBuilder(
                    "internal.dbms.index.vector.population_merge_factor", INT, 1000)
            .build();

    @Internal
    @Description("If a merged segment will be more than this percentage of the total size of the index, "
            + "leave the segment as non-compound file even if compound file is enabled. "
            + "Set to 1.0 to always use CFS regardless of merge size.")
    public static final Setting<Double> lucene_nocfs_ratio =
            newBuilder("internal.dbms.index.lucene.nocfs.ratio", DOUBLE, 1.0).build();

    @Internal
    @Description("If a merged segment will be more than this size in mb, "
            + "leave the segment as non-compound file even if compound file is enabled. "
            + "By default no upper limit is specified.")
    public static final Setting<Double> lucene_max_cfs_segment_size_mb = newBuilder(
                    "internal.dbms.index.lucene.max_cfs_segment_size_mb", DOUBLE, Double.POSITIVE_INFINITY)
            .build();

    @Internal
    @Description("Sets the merge policy to use for Lucene")
    public static final Setting<MergePolicyOption> lucene_merge_policy = newBuilder(
                    "internal.dbms.index.lucene.merge_policy", ofEnum(MergePolicyOption.class), LOG_BYTE_SIZED)
            .build();

    @Internal
    @Description("Sets the merge policy to use for Vector Search")
    public static final Setting<MergePolicyOption> vector_merge_policy = newBuilder(
                    "internal.dbms.index.vector.merge_policy", ofEnum(MergePolicyOption.class), LOG_BYTE_SIZED)
            .build();

    @Internal
    @Description("Sets the minimum size for the lowest level segments. Any segments below this size are candidates for"
            + " full-flush merges and be merged more aggressively in order to avoid having a long tail of small"
            + " segments. Large values of this parameter increase the merging cost during indexing if you flush"
            + " small segments. Only used if `internal.dbms.index.lucene.merge_policy` is set to LOG_BYTE_SIZE.")
    public static final Setting<Double> lucene_min_merge =
            newBuilder("internal.dbms.index.lucene.min_merge", DOUBLE, 0.1).build();

    @Internal
    @Description(
            "Determines the largest segment (measured by total byte size of the segment's files, in MB) that may be"
                    + " merged with other segments. Small values (e. g., less than 50 MB) are best for interactive"
                    + " indexing, as this limits the length of pauses while indexing to a few seconds. Larger values are"
                    + " best for batched indexing and speedier searches. Only used if"
                    + " `internal.dbms.index.lucene.merge_policy` is set to LOG_BYTE_SIZE.")
    public static final Setting<Double> lucene_max_merge =
            newBuilder("internal.dbms.index.lucene.max_merge", DOUBLE, 2048.0D).build();

    @Internal
    @Description(
            "Sets the allowed number of segments per tier. Smaller values mean more merging but fewer segments. Only"
                    + " used if `internal.dbms.index.lucene.merge_policy` is set to TIERED.")
    public static final Setting<Double> lucene_segments_per_tier = newBuilder(
                    "internal.dbms.index.lucene.segments_per_tiers", DOUBLE, 8.0)
            .build();

    @Internal
    @Description(
            "Sets the allowed number of segments per tier. Smaller values mean more merging but fewer segments. Only"
                    + " used if `internal.dbms.index.lucene.merge_policy` is set to TIERED.")
    public static final Setting<Double> vector_segments_per_tier = newBuilder(
                    "internal.dbms.index.vector.segments_per_tiers", DOUBLE, 8.0)
            .build();

    @Internal
    @Description("Maximum number of segments to be merged at a time during \"normal\" merging Only"
            + " used if `internal.dbms.index.lucene.merge_policy` is set to TIERED.")
    public static final Setting<Integer> lucene_max_merge_at_once =
            newBuilder("internal.dbms.index.lucene.max_merge_at_once", INT, 10).build();

    @Internal
    @Description("Maximum number of segments to be merged at a time during \"normal\" merging Only"
            + " used if `internal.dbms.index.lucene.merge_policy` is set to TIERED.")
    public static final Setting<Integer> vector_max_merge_at_once =
            newBuilder("internal.dbms.index.vector.max_merge_at_once", INT, 10).build();

    @Internal
    @Description("Determines the amount of RAM (in MiB) that may be used for buffering added documents and deletions "
            + "before they are flushed to the Directory. Generally for faster indexing performance it's best "
            + "to flush by RAM usage instead of document count and use as large a RAM buffer as you can.")
    public static final Setting<Double> lucene_standard_ram_buffer_size = newBuilder(
                    "internal.dbms.index.lucene.standard_ram_buffer_size",
                    DOUBLE,
                    LuceneIndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB)
            .build();

    @Internal
    @Description("Determines the amount of RAM that may be used for buffering added documents and deletions "
            + "before they are flushed to the Directory. Generally for faster indexing performance it's best "
            + "to flush by RAM usage instead of document count and use as large a RAM buffer as you can. "
            + "This is only used during the creation of the index.")
    public static final Setting<Double> lucene_population_ram_buffer_size = newBuilder(
                    "internal.dbms.index.lucene.population_ram_buffer_size", DOUBLE, 50.0D)
            .build();

    @Internal
    @Description("Determines the amount of RAM that may be used for buffering added documents and deletions "
            + "before they are flushed to the Directory. Generally for faster indexing performance it's best "
            + "to flush by RAM usage instead of document count and use as large a RAM buffer as you can. "
            + "This is only used for vector indexes during the creation of the index.")
    public static final Setting<Double> vector_population_ram_buffer_size = newBuilder(
                    "internal.dbms.index.vector.population_ram_buffer_size", DOUBLE, 1.0D)
            .build();

    @Internal
    @Description("Sets the merge scheduler used by this writer. The default is ConcurrentMergeScheduler. "
            + "If 'false' separate threads will be used for merge, if 'true' the merges will be done "
            + "sequentially by the current thread.")
    public static final Setting<Boolean> lucene_population_serial_merge_scheduler = newBuilder(
                    "internal.dbms.index.lucene.population_serial_merge_scheduler", BOOL, TRUE)
            .build();

    @Internal
    @Description("Controls whether any newly created indexes backed by lucene are populated during a sharded import. "
            + "If 'true', the index will be created but NOT populated during import. Population would then occur "
            + "during the shards initial recovery when the sharded database is created.")
    public static final Setting<Boolean> lucene_skip_population_during_sharded_import = newBuilder(
                    "internal.dbms.index.lucene.skip_population_during_sharded_import", BOOL, TRUE)
            .build();
}
