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

class GenericNativeIndexReader extends NativeIndexReader<GenericKey,NativeIndexValue>
{
    private final IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings;
    private final SpaceFillingCurveConfiguration configuration;

    GenericNativeIndexReader( GBPTree<GenericKey,NativeIndexValue> tree, IndexLayout<GenericKey,NativeIndexValue> layout,
            IndexDescriptor descriptor, IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings,
            SpaceFillingCurveConfiguration configuration )
    {
        super( tree, layout, descriptor );
        this.spaceFillingCurveSettings = spaceFillingCurveSettings;
        this.configuration = configuration;
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        for ( IndexQuery predicate : predicates )
        {
            ValueGroup valueGroup = predicate.valueGroup();
            if ( valueGroup == ValueGroup.GEOMETRY_ARRAY || valueGroup == ValueGroup.GEOMETRY )
            {
                return false;
            }
        }
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
        IndexQuery.GeometryRangePredicate geometryRangePredicate = getGeometryRangePredicateIfAny( query );
        if ( geometryRangePredicate != null )
        {
            validateQuery( indexOrder, query );
            try
            {
                // If there's a GeometryRangeQuery among the predicates then this query changes from a straight-forward: build from/to and seek...
                // into a query that is split into multiple sub-queries. Predicates both before and after will have to be accompanied each sub-query.
                BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( client, descriptor.schema().getPropertyIds() );
                client.initialize( descriptor, multiProgressor, query, indexOrder, needsValues );
                double[] from = geometryRangePredicate.from() == null ? null : geometryRangePredicate.from().coordinate();
                double[] to = geometryRangePredicate.to() == null ? null : geometryRangePredicate.to().coordinate();
                CoordinateReferenceSystem crs = geometryRangePredicate.crs();
                SpaceFillingCurve curve = spaceFillingCurveSettings.forCrs( crs, false );
                List<SpaceFillingCurve.LongRange> ranges = curve.getTilesIntersectingEnvelope( from, to, configuration );
                for ( SpaceFillingCurve.LongRange range : ranges )
                {
                    // Here's a sub-query that we'll have to do for this geometry range. Build this query from all predicates
                    // and when getting to the geometry range predicate that sparked these sub-query chenanigans, swap in this sub-query in its place.
                    GenericKey treeKeyFrom = layout.newKey();
                    GenericKey treeKeyTo = layout.newKey();
                    initializeFromToKeys( treeKeyFrom, treeKeyTo );
                    boolean needFiltering = initializeRangeForGeometrySubQuery( treeKeyFrom, treeKeyTo, query, crs, range );
                    startSeekForInitializedRange( multiProgressor, treeKeyFrom, treeKeyTo, query, indexOrder, needFiltering, needsValues );
                }
            }
            catch ( IllegalArgumentException e )
            {
                // Invalid query ranges will cause this state (eg. min>max)
                client.initialize( descriptor, IndexProgressor.EMPTY, query, indexOrder, needsValues );
            }
        }
        else
        {
            super.query( client, indexOrder, needsValues, query );
        }
    }

    /**
     * Initializes {@code treeKeyFrom} and {@code treeKeyTo} from the {@link IndexQuery query}.
     * Geometry range queries makes an otherwise straight-forward key construction complex in that a geometry range internally is performed
     * by executing multiple sub-range queries to the index. Each of those sub-range queries still needs to construct the full composite key -
     * in the case of a composite index. Therefore this method can be called either with null or non-null {@code crs} and {@code range} and
     * constructing a key when coming across a {@link IndexQuery.GeometryRangePredicate} will use the provided crs/range instead
     * of the predicate, where the specific range is one out of many sub-ranges calculated from the {@link IndexQuery.GeometryRangePredicate}
     * by the caller.
     *
     * @param treeKeyFrom the "from" key to construct from the query.
     * @param treeKeyTo the "to" key to construct from the query.
     * @param query the query to construct keys from to later send to {@link GBPTree} when reading.
     * @param crs {@link CoordinateReferenceSystem} for the specific {@code range}, if range is specified too.
     * @param range sub-range of a larger {@link IndexQuery.GeometryRangePredicate} to use instead of {@link IndexQuery.GeometryRangePredicate}
     * in the query.
     * @return {@code true} if filtering is needed for the results from the reader, otherwise {@code false}.
     */
    private boolean initializeRangeForGeometrySubQuery( GenericKey treeKeyFrom, GenericKey treeKeyTo,
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
                    // Use the supplied SpaceFillingCurve range instead of the GeometryRangePredicate because at the time of calling this method
                    // the original geometry range have been split up into multiple sub-ranges and this invocation is for one of those sub-ranges.
                    // We can not take query inclusion / exclusion into consideration here because then we risk missing border values. Always use
                    // Inclusion.LOW / HIGH respectively and filter out points later on.
                    treeKeyFrom.stateSlot( i ).writePointDerived( crs, range.min, LOW );
                    treeKeyTo.stateSlot( i ).writePointDerived( crs, range.max + 1, HIGH );
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
                treeKeyFrom.stateSlot( i ).initAsPrefixLow( prefixPredicate.prefix() );
                treeKeyTo.stateSlot( i ).initAsPrefixHigh( prefixPredicate.prefix() );
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
    boolean initializeRangeForQuery( GenericKey treeKeyFrom, GenericKey treeKeyTo, IndexQuery[] query )
    {
        return initializeRangeForGeometrySubQuery( treeKeyFrom, treeKeyTo, query, null, null );
    }

    private static void initFromForRange( int stateSlot, RangePredicate<?> rangePredicate, GenericKey treeKeyFrom )
    {
        Value fromValue = rangePredicate.fromValue();
        if ( fromValue == Values.NO_VALUE )
        {
            treeKeyFrom.initValueAsLowest( stateSlot, rangePredicate.valueGroup() );
        }
        else
        {
            treeKeyFrom.initFromValue( stateSlot, fromValue, fromInclusion( rangePredicate ) );
            treeKeyFrom.setCompareId( true );
        }
    }

    private static void initToForRange( int stateSlot, RangePredicate<?> rangePredicate, GenericKey treeKeyTo )
    {
        Value toValue = rangePredicate.toValue();
        if ( toValue == Values.NO_VALUE )
        {
            treeKeyTo.initValueAsHighest( stateSlot, rangePredicate.valueGroup() );
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
