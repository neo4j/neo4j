package org.neo4j.kernel.impl.index.schema.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * A combination of {@link ConfiguredSpaceFillingCurveSettingsCache}, which contains all settings from {@link Config},
 * but also settings for a specific index. Settings for a specific index can change over time as new {@link CoordinateReferenceSystem}
 * are used in this index.
 */
public class IndexSpecificSpaceFillingCurveSettingsCache
{
    private final ConfiguredSpaceFillingCurveSettingsCache globalConfigCache;
    private final ConcurrentMap<CoordinateReferenceSystem,SpaceFillingCurveSettings> indexConfigCache = new ConcurrentHashMap<>();

    public IndexSpecificSpaceFillingCurveSettingsCache(
            ConfiguredSpaceFillingCurveSettingsCache globalConfigCache,
            Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> indexConfigCache )
    {
        this.globalConfigCache = globalConfigCache;
        this.indexConfigCache.putAll( indexConfigCache );
    }

    /**
     * Gets {@link SpaceFillingCurve} for a particular coordinate reference system's crsTableId and code point.
     *
     * @param crsTableId table id of the {@link CoordinateReferenceSystem}.
     * @param crsCodePoint code of the {@link CoordinateReferenceSystem}.
     * @param assignToIndexIfNotYetAssigned whether or not to make a snapshot of this setting index-specific if this is the
     * first time it's accessed for this index. It will then show up in {@link #visitIndexSpecificSettings(SettingVisitor)}.
     * @return the {@link SpaceFillingCurve} for the given coordinate reference system.
     */
    public SpaceFillingCurve forCrs( int crsTableId, int crsCodePoint, boolean assignToIndexIfNotYetAssigned )
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( crsTableId, crsCodePoint );

        // Index-specific
        SpaceFillingCurveSettings specificSetting = indexConfigCache.get( crs );
        if ( specificSetting != null )
        {
            return specificSetting.curve();
        }

        // Global config
        SpaceFillingCurveSettings configuredSetting = fromConfig( crs );
        if ( assignToIndexIfNotYetAssigned )
        {
            indexConfigCache.put( crs, configuredSetting );
        }
        return configuredSetting.curve();
    }

    /**
     * Mostly for checkpoints to serialize index-specific settings into the index header.
     */
    public void visitIndexSpecificSettings( SettingVisitor visitor )
    {
        indexConfigCache.forEach( (crs, settings) -> visitor.visit( crs, settings ) );
    }

    private SpaceFillingCurveSettings fromConfig( CoordinateReferenceSystem crs )
    {
        SpaceFillingCurveSettings global = globalConfigCache.forCRS( crs );
        if ( global != null )
        {
            return global;
        }

        // Fall back to creating one (TODO cache this?)
        return SpaceFillingCurveSettingsFactory.fromConfig( globalConfigCache.getMaxBits(), new EnvelopeSettings( crs ) );
    }

    public interface SettingVisitor
    {
        void visit( CoordinateReferenceSystem crs, SpaceFillingCurveSettings settings );
    }
}
