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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory.getConfiguredSpaceFillingCurveConfiguration;

import java.nio.file.OpenOption;
import java.util.Map;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.ValueCategory;

public class PointIndexProvider extends NativeIndexProvider<PointKey, PointLayout> {
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor("point", "1.0");
    public static final IndexCapability CAPABILITY = new PointIndexCapability();

    // Ignore everything except GEOMETRY values
    static final IndexUpdateIgnoreStrategy UPDATE_IGNORE_STRATEGY =
            values -> values[0].valueGroup().category() != ValueCategory.GEOMETRY;
    /**
     * Cache of all setting for various specific CRS's found in the config at instantiation of this provider.
     * The config is read once and all relevant CRS configs cached here.
     */
    private final ConfiguredSpaceFillingCurveSettingsCache configuredSettings;

    /**
     * A space filling curve configuration used when reading spatial index values.
     */
    private final SpaceFillingCurveConfiguration configuration;

    private final boolean archiveFailedIndex;
    private final Config config;

    public PointIndexProvider(
            DatabaseIndexContext databaseIndexContext,
            IndexDirectoryStructure.Factory directoryStructureFactory,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            Config config) {
        super(databaseIndexContext, DESCRIPTOR, directoryStructureFactory, recoveryCleanupWorkCollector, config);
        this.configuredSettings = new ConfiguredSpaceFillingCurveSettingsCache(config);
        this.configuration = getConfiguredSpaceFillingCurveConfiguration(config);
        this.archiveFailedIndex = config.get(GraphDatabaseInternalSettings.archive_failed_index);
        this.config = config;
    }

    @Override
    PointLayout layout(IndexDescriptor descriptor) {
        IndexConfig indexConfig = descriptor.getIndexConfig();
        Map<CoordinateReferenceSystem, SpaceFillingCurveSettings> settings =
                SpatialIndexConfig.extractSpatialConfig(indexConfig);
        IndexSpecificSpaceFillingCurveSettings spatialSettings = new IndexSpecificSpaceFillingCurveSettings(settings);
        return new PointLayout(spatialSettings);
    }

    @Override
    protected IndexPopulator newIndexPopulator(
            IndexFiles indexFiles,
            PointLayout layout,
            IndexDescriptor descriptor,
            ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions) {
        return new PointBlockBasedIndexPopulator(
                databaseIndexContext,
                indexFiles,
                layout,
                descriptor,
                layout.getSpaceFillingCurveSettings(),
                configuration,
                archiveFailedIndex,
                bufferFactory,
                config,
                memoryTracker,
                BlockBasedIndexPopulator.NO_MONITOR,
                openOptions);
    }

    @Override
    protected IndexAccessor newIndexAccessor(
            IndexFiles indexFiles,
            PointLayout layout,
            IndexDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly) {
        return new PointIndexAccessor(
                databaseIndexContext,
                indexFiles,
                layout,
                recoveryCleanupWorkCollector,
                descriptor,
                layout.getSpaceFillingCurveSettings(),
                configuration,
                openOptions,
                readOnly);
    }

    @Override
    public IndexDescriptor completeConfiguration(
            IndexDescriptor index, StorageEngineIndexingBehaviour indexingBehaviour) {
        IndexConfig indexConfig = index.getIndexConfig();
        indexConfig = completeSpatialConfiguration(indexConfig);
        index = index.withIndexConfig(indexConfig);
        if (index.getCapability().equals(IndexCapability.NO_CAPABILITY)) {
            index = index.withIndexCapability(CAPABILITY);
        }
        return index;
    }

    private IndexConfig completeSpatialConfiguration(IndexConfig indexConfig) {
        for (CoordinateReferenceSystem crs : CoordinateReferenceSystem.all()) {
            SpaceFillingCurveSettings spaceFillingCurveSettings = configuredSettings.forCRS(crs);
            indexConfig = SpatialIndexConfig.addSpatialConfig(indexConfig, crs, spaceFillingCurveSettings);
        }
        return indexConfig;
    }

    @Override
    public void validatePrototype(IndexPrototype prototype) {
        IndexType indexType = prototype.getIndexType();
        if (indexType != IndexType.POINT) {
            String providerName = getProviderDescriptor().name();
            throw new IllegalArgumentException("The '" + providerName + "' index provider does not support " + indexType
                    + " indexes: " + prototype);
        }
        if (!(prototype.schema().isSchemaDescriptorType(LabelSchemaDescriptor.class)
                || prototype.schema().isSchemaDescriptorType(RelationTypeSchemaDescriptor.class))) {
            throw new IllegalArgumentException("The " + prototype.schema()
                    + " index schema is not a point index schema, which it is required to be for the '"
                    + getProviderDescriptor().name() + "' index provider to be able to create an index.");
        }
        if (!prototype.getIndexProvider().equals(DESCRIPTOR)) {
            throw new IllegalArgumentException("The '" + getProviderDescriptor().name()
                    + "' index provider does not support " + prototype.getIndexProvider() + " indexes: " + prototype);
        }
        if (prototype.isUnique()) {
            throw new IllegalArgumentException("The '" + getProviderDescriptor().name()
                    + "' index provider does not support uniqueness indexes: " + prototype);
        }
        if (prototype.schema().getPropertyIds().length != 1) {
            throw new IllegalArgumentException("The '" + getProviderDescriptor().name()
                    + "' index provider does not support composite indexes: " + prototype);
        }

        IndexConfig indexConfig = prototype.getIndexConfig();
        indexConfig = completeSpatialConfiguration(indexConfig);
        try {
            SpatialIndexConfig.validateSpatialConfig(indexConfig);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid spatial index settings.", e);
        }
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.POINT;
    }

    private static class PointIndexCapability implements IndexCapability {
        @Override
        public boolean supportsOrdering() {
            return false;
        }

        @Override
        public boolean supportsReturningValues() {
            // The point index has values for all the queries it supports.
            return true;
        }

        @Override
        public boolean areValueCategoriesAccepted(ValueCategory... valueCategories) {
            Preconditions.requireNonEmpty(valueCategories);
            Preconditions.requireNoNullElements(valueCategories);
            return valueCategories.length == 1 && valueCategories[0] == ValueCategory.GEOMETRY;
        }

        @Override
        public boolean isQuerySupported(IndexQueryType queryType, ValueCategory valueCategory) {
            if (queryType == IndexQueryType.ALL_ENTRIES) {
                return true;
            }

            if (!areValueCategoriesAccepted(valueCategory)) {
                return false;
            }

            return switch (queryType) {
                case EXACT, BOUNDING_BOX -> true;
                default -> false;
            };
        }

        @Override
        public double getCostMultiplier(IndexQueryType... queryTypes) {
            return COST_MULTIPLIER_STANDARD;
        }

        @Override
        public boolean supportPartitionedScan(IndexQuery... queries) {
            Preconditions.requireNonEmpty(queries);
            Preconditions.requireNoNullElements(queries);
            return false;
        }
    }
}
