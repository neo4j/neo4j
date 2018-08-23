/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import java.util.List;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.RangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringPrefixPredicate;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.schema.BridgingIndexProgressor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.HIGH;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.LOW;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

class GenericNativeIndexReader extends NativeIndexReader<CompositeGenericKey,NativeIndexValue>
{
    private final IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings;
    private final SpaceFillingCurveConfiguration configuration;

    GenericNativeIndexReader( GBPTree<CompositeGenericKey,NativeIndexValue> tree, IndexLayout<CompositeGenericKey,NativeIndexValue> layout,
            IndexSamplingConfig samplingConfig, IndexDescriptor descriptor, IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings,
            SpaceFillingCurveConfiguration configuration )
    {
        super( tree, layout, samplingConfig, descriptor );
        this.spaceFillingCurveSettings = spaceFillingCurveSettings;
        this.configuration = configuration;
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        // TODO except spatial tho
        return true;
    }

    @Override
    void validateQuery( IndexOrder indexOrder, IndexQuery[] predicates )
    {
        CapabilityValidator.validateQuery( GenericNativeIndexProvider.CAPABILITY, indexOrder, predicates );
    }

    @Override
    public void query( IndexProgressor.NodeValueClient client, IndexOrder indexOrder, boolean needsValues, IndexQuery... query )
    {
        validateQuery( indexOrder, query );

        IndexQuery.GeometryRangePredicate geometryRangePredicate = getGeometryRangePredicateIfAny( query );
        if ( geometryRangePredicate != null )
        {
            try
            {
                // If there's a GeometryRangeQuery among the predicates then this query changes from a straight-forward: build from/to and seek...
                // into a query that is split into multiple sub-queries. Predicates both before and after will have to be accompanied each sub-query.
                BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( client, descriptor.schema().getPropertyIds() );
                client.initialize( descriptor, multiProgressor, query, false );
                double[] from = geometryRangePredicate.from() == null ? null : geometryRangePredicate.from().coordinate();
                double[] to = geometryRangePredicate.to() == null ? null : geometryRangePredicate.to().coordinate();
                CoordinateReferenceSystem crs = geometryRangePredicate.crs();
                SpaceFillingCurve curve = spaceFillingCurveSettings.forCrs( crs, false );
                List<SpaceFillingCurve.LongRange> ranges = curve.getTilesIntersectingEnvelope( from, to, configuration );
                for ( SpaceFillingCurve.LongRange range : ranges )
                {
                    // Here's a sub-query that we'll have to do for this geometry range. Build this query from all predicates
                    // and when getting to the geometry range predicate that sparked these sub-query chenanigans, swap in this sub-query in its place.
                    CompositeGenericKey treeKeyFrom = layout.newKey();
                    CompositeGenericKey treeKeyTo = layout.newKey();
                    initializeFromToKeys( treeKeyFrom, treeKeyTo );
                    boolean needFiltering = initializeRangeForGeometrySubQuery( multiProgressor, treeKeyFrom, treeKeyTo, query, crs, range );

                    // TODO needsValues==true could be problematic, no?
                    startSeekForInitializedRange( multiProgressor, treeKeyFrom, treeKeyTo, query, needFiltering, needsValues );
                }
            }
            catch ( IllegalArgumentException e )
            {
                // Invalid query ranges will cause this state (eg. min>max)
                client.initialize( descriptor, IndexProgressor.EMPTY, query, false );
            }
        }
        else
        {
            CompositeGenericKey treeKeyFrom = layout.newKey();
            CompositeGenericKey treeKeyTo = layout.newKey();
            initializeFromToKeys( treeKeyFrom, treeKeyTo );

            boolean needFilter = initializeRangeForQuery( client, treeKeyFrom, treeKeyTo, query );
            startSeekForInitializedRange( client, treeKeyFrom, treeKeyTo, query, needFilter, needsValues );
        }
    }

    private boolean initializeRangeForGeometrySubQuery( IndexProgressor.NodeValueClient client, CompositeGenericKey treeKeyFrom, CompositeGenericKey treeKeyTo,
            IndexQuery[] query, CoordinateReferenceSystem crs, SpaceFillingCurve.LongRange range )
    {
        boolean needsFiltering = false;
        for ( int i = 0; i < query.length; i++ )
        {
            IndexQuery predicate = query[i];
            switch ( predicate.type() )
            {
            case exists:
                treeKeyFrom.initValueAsLowest( i, ValueGroup.UNKNOWN );
                treeKeyTo.initValueAsHighest( i, ValueGroup.UNKNOWN );
                break;
            case exact:
                ExactPredicate exactPredicate = (ExactPredicate) predicate;
                treeKeyFrom.initFromValue( i, exactPredicate.value(), NEUTRAL );
                treeKeyTo.initFromValue( i, exactPredicate.value(), NEUTRAL );
                break;
            case range:
                if ( isGeometryRangeQuery( predicate ) )
                {
                    // Use the supplied SpaceFillingCurve range instead
                    treeKeyFrom.initFromDerivedSpatialValue( i, crs, range.min, fromInclusion( (RangePredicate<?>) predicate ) );
                    treeKeyTo.initFromDerivedSpatialValue( i, crs, range.max + 1, toInclusion( (RangePredicate<?>) predicate ) );
                }
                else
                {
                    RangePredicate<?> rangePredicate = (RangePredicate<?>) predicate;
                    initFromForRange( i, rangePredicate, treeKeyFrom );
                    initToForRange( i, rangePredicate, treeKeyTo );
                }
                break;
            case stringPrefix:
                StringPrefixPredicate prefixPredicate = (StringPrefixPredicate) predicate;
                treeKeyFrom.initAsPrefixLow( i, prefixPredicate.prefix() );
                treeKeyTo.initAsPrefixHigh( i, prefixPredicate.prefix() );
                break;
            case stringSuffix:
            case stringContains:
                treeKeyFrom.initValueAsLowest( i, ValueGroup.TEXT );
                treeKeyTo.initValueAsHighest( i, ValueGroup.TEXT );
                needsFiltering = true;
                break;
            default:
                throw new IllegalArgumentException( "IndexQuery of type " + predicate.type() + " is not supported." );
            }
        }
        return needsFiltering;
    }

    @Override
    boolean initializeRangeForQuery( IndexProgressor.NodeValueClient client, CompositeGenericKey treeKeyFrom, CompositeGenericKey treeKeyTo,
            IndexQuery[] query )
    {
        return initializeRangeForGeometrySubQuery( client, treeKeyFrom, treeKeyTo, query, null, null );
    }

    private static void initFromForRange( int stateSlot, RangePredicate<?> rangePredicate, CompositeGenericKey treeKeyFrom )
    {
        Value fromValue = rangePredicate.fromValue();
        if ( fromValue == Values.NO_VALUE )
        {
            treeKeyFrom.initValueAsLowest( stateSlot, ValueGroup.UNKNOWN );
        }
        else
        {
            treeKeyFrom.initFromValue( stateSlot, fromValue, fromInclusion( rangePredicate ) );
            treeKeyFrom.setCompareId( true );
        }
    }

    private static void initToForRange( int stateSlot, RangePredicate<?> rangePredicate, CompositeGenericKey treeKeyTo )
    {
        Value toValue = rangePredicate.toValue();
        if ( toValue == Values.NO_VALUE )
        {
            treeKeyTo.initValueAsHighest( stateSlot, ValueGroup.UNKNOWN );
        }
        else
        {
            treeKeyTo.initFromValue( stateSlot, toValue, toInclusion( rangePredicate ) );
            treeKeyTo.setCompareId( true );
        }
    }

    private static NativeIndexKey.Inclusion fromInclusion( RangePredicate<?> rangePredicate )
    {
        return rangePredicate.fromInclusive() ? LOW : HIGH;
    }

    private static NativeIndexKey.Inclusion toInclusion( RangePredicate<?> rangePredicate )
    {
        return rangePredicate.toInclusive() ? HIGH : LOW;
    }

    private IndexQuery.GeometryRangePredicate getGeometryRangePredicateIfAny( IndexQuery[] predicates )
    {
        for ( IndexQuery predicate : predicates )
        {
            if ( isGeometryRangeQuery( predicate ) )
            {
                return (IndexQuery.GeometryRangePredicate) predicate;
            }
        }
        return null;
    }

    private boolean isGeometryRangeQuery( IndexQuery predicate )
    {
        return predicate instanceof IndexQuery.GeometryRangePredicate;
    }

}
