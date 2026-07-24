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
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.nativeimpl.NativeIndexCapability;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.ValueCategory;

public class PointIndexProvider extends NativeIndexProvider<PointKey, PointLayout> {
    public static final IndexCapability CAPABILITY = NativeIndexCapability.POINT;

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
            Config config,
            LogProvider logProvider) {
        super(
                databaseIndexContext,
                AllIndexProviderDescriptors.POINT_DESCRIPTOR,
                directoryStructureFactory,
                recoveryCleanupWorkCollector,
                config,
                logProvider);
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
            ElementIdMapper elementIdMapper,
            ImmutableSet<OpenOption> openOptions,
            IndexPopulator.Configuration configuration) {
        return new PointBlockBasedIndexPopulator(
                databaseIndexContext,
                indexFiles,
                layout,
                descriptor,
                layout.getSpaceFillingCurveSettings(),
                this.configuration,
                archiveFailedIndex,
                bufferFactory,
                config,
                memoryTracker,
                BlockBasedIndexPopulator.NO_MONITOR,
                openOptions,
                logProvider,
                tokenNameLookup,
                configuration);
    }

    @Override
    IndexAccessor newIndexAccessor(
            IndexFiles indexFiles,
            PointLayout layout,
            IndexDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            ElementIdMapper elementIdMapper,
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
                readOnly,
                logProvider,
                tokenNameLookup);
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
    public IndexPrototype validatePrototype(IndexPrototype prototype) {
        IndexType indexType = prototype.getIndexType();
        String providerName = getProviderDescriptor().name();
        if (indexType != IndexType.POINT) {
            throw InvalidArgumentException.invalidIndexInput(
                    indexType.toString(),
                    providerName,
                    "The '%s' index provider does not support %s indexes: %s"
                            .formatted(providerName, indexType, prototype));
        }
        if (!(prototype.schema().isLabelSchemaDescriptor()
                || prototype.schema().isRelationshipTypeSchemaDescriptor())) {
            throw InvalidArgumentException.invalidIndexInput(
                    indexType.toString(),
                    providerName,
                    "The " + prototype.schema()
                            + " index schema is not a point index schema, which it is required to be for the '"
                            + providerName + "' index provider to be able to create an index.");
        }
        if (!prototype.getIndexProvider().equals(AllIndexProviderDescriptors.POINT_DESCRIPTOR)) {
            throw InvalidArgumentException.invalidIndexInput(
                    indexType.toString(),
                    providerName,
                    "The " + prototype.schema() + " index schema is not a full-text index schema, "
                            + "which it is required to be for the '" + providerName
                            + "' index provider to be able to create an index.");
        }
        if (prototype.isUnique()) {
            throw InvalidArgumentException.invalidIndexInput(
                    indexType.toString(),
                    providerName,
                    "The '" + providerName + "' index provider does not support uniqueness indexes: " + prototype);
        }
        if (prototype.schema().getPropertyIds().length != 1) {
            throw InvalidArgumentException.invalidIndexInput(
                    indexType.toString(),
                    providerName,
                    "The '" + getProviderDescriptor().name() + "' index provider does not support composite indexes: "
                            + prototype);
        }

        IndexConfig indexConfig = prototype.getIndexConfig();
        indexConfig = completeSpatialConfiguration(indexConfig);
        try {
            SpatialIndexConfig.validateSpatialConfig(indexConfig);
        } catch (IllegalArgumentException e) {
            throw InvalidArgumentException.invalidArgument("Invalid spatial index settings.", e);
        }
        return prototype;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.POINT;
    }
}
