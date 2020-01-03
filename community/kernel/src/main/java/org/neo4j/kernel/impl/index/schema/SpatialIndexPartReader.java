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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;

import org.neo4j.cursor.RawCursor;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.GeometryRangePredicate;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.schema.BridgingIndexProgressor;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class SpatialIndexPartReader<VALUE extends NativeIndexValue> extends NativeIndexReader<SpatialIndexKey,VALUE>
{
    private final SpatialLayout spatial;
    private final SpaceFillingCurveConfiguration configuration;

    SpatialIndexPartReader( GBPTree<SpatialIndexKey,VALUE> tree, IndexLayout<SpatialIndexKey,VALUE> layout,
            IndexDescriptor descriptor, SpaceFillingCurveConfiguration configuration )
    {
        super( tree, layout, descriptor );
        spatial = (SpatialLayout) layout;
        this.configuration = configuration;
    }

    @Override
    void validateQuery( IndexOrder indexOrder, IndexQuery[] predicates )
    {
        if ( predicates.length != 1 )
        {
            throw new UnsupportedOperationException( "Spatial index doesn't handle composite queries" );
        }

        CapabilityValidator.validateQuery( SpatialIndexProvider.CAPABILITY, indexOrder, predicates );
    }

    @Override
    boolean initializeRangeForQuery( SpatialIndexKey treeKeyFrom, SpatialIndexKey treeKeyTo, IndexQuery[] predicates )
    {
        throw new UnsupportedOperationException( "Cannot initialize 1D range in multidimensional spatial index reader" );
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, boolean needsValues, IndexQuery... predicates )
    {
        // Spatial does not support providing values
        if ( needsValues )
        {
            throw new IllegalStateException( "Spatial index does not support providing values" );
        }

        validateQuery( indexOrder, predicates );
        IndexQuery predicate = predicates[0];

        SpatialIndexKey treeKeyFrom = layout.newKey();
        SpatialIndexKey treeKeyTo = layout.newKey();
        initializeKeys( treeKeyFrom, treeKeyTo );

        switch ( predicate.type() )
        {
        case exists:
            startSeekForExists( treeKeyFrom, treeKeyTo, cursor, predicate );
            break;
        case exact:
            startSeekForExact( treeKeyFrom, treeKeyTo, cursor, ((ExactPredicate) predicate).value(), predicate );
            break;
        case range:
            GeometryRangePredicate rangePredicate = (GeometryRangePredicate) predicate;
            if ( !rangePredicate.crs().equals( spatial.crs ) )
            {
                throw new IllegalArgumentException(
                        "IndexQuery on spatial index with mismatching CoordinateReferenceSystem: " + rangePredicate.crs() + " != " + spatial.crs );
            }
            startSeekForRange( cursor, rangePredicate, predicates );
            break;
        default:
            throw new IllegalArgumentException( "IndexQuery of type " + predicate.type() + " is not supported." );
        }
    }

    @Override
    public void distinctValues( IndexProgressor.NodeValueClient client, NodePropertyAccessor propertyAccessor, boolean needsValues )
    {
        // This is basically a version of the basic implementation, but with added consulting of the PropertyAccessor
        // since these are lossy spatial values.
        SpatialIndexKey lowest = layout.newKey();
        lowest.initialize( Long.MIN_VALUE );
        lowest.initValuesAsLowest();
        SpatialIndexKey highest = layout.newKey();
        highest.initialize( Long.MAX_VALUE );
        highest.initValuesAsHighest();
        try
        {
            RawCursor<Hit<SpatialIndexKey,VALUE>,IOException> seeker = tree.seek( lowest, highest );
            Comparator<SpatialIndexKey> comparator =
                    new PropertyLookupFallbackComparator<>( layout, propertyAccessor, descriptor.schema().getPropertyId() );
            NativeDistinctValuesProgressor<SpatialIndexKey,VALUE> progressor =
                    new NativeDistinctValuesProgressor<SpatialIndexKey,VALUE>( seeker, client, openSeekers, layout, comparator )
                    {
                        @Override
                        Value[] extractValues( SpatialIndexKey key )
                        {
                            try
                            {
                                return new Value[]{propertyAccessor.getNodePropertyValue( key.getEntityId(), descriptor.schema().getPropertyId() )};
                            }
                            catch ( EntityNotFoundException e )
                            {
                                // We couldn't get the value due to the entity not being there. Concurrently deleted?
                                return null;
                            }
                        }
                    };
            client.initialize( descriptor, progressor, new IndexQuery[0], IndexOrder.NONE, false );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void startSeekForExists( SpatialIndexKey treeKeyFrom, SpatialIndexKey treeKeyTo, IndexProgressor.NodeValueClient client, IndexQuery... predicates )
    {
        treeKeyFrom.initValueAsLowest( ValueGroup.GEOMETRY );
        treeKeyTo.initValueAsHighest( ValueGroup.GEOMETRY );
        startSeekForInitializedRange( client, treeKeyFrom, treeKeyTo, predicates, IndexOrder.NONE, false, false );
    }

    private void startSeekForExact( SpatialIndexKey treeKeyFrom, SpatialIndexKey treeKeyTo, IndexProgressor.NodeValueClient client, Value value,
            IndexQuery... predicates )
    {
        treeKeyFrom.from( value );
        treeKeyTo.from( value );
        startSeekForInitializedRange( client, treeKeyFrom, treeKeyTo, predicates, IndexOrder.NONE, false, false );
    }

    private void startSeekForRange( IndexProgressor.NodeValueClient client, GeometryRangePredicate rangePredicate, IndexQuery[] query )
    {
        try
        {
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( client, descriptor.schema().getPropertyIds() );
            client.initialize( descriptor, multiProgressor, query, IndexOrder.NONE, false );
            SpaceFillingCurve curve = spatial.getSpaceFillingCurve();
            double[] from = rangePredicate.from() == null ? null : rangePredicate.from().coordinate();
            double[] to = rangePredicate.to() == null ? null : rangePredicate.to().coordinate();
            List<SpaceFillingCurve.LongRange> ranges = curve.getTilesIntersectingEnvelope( from, to, configuration );
            for ( SpaceFillingCurve.LongRange range : ranges )
            {
                SpatialIndexKey treeKeyFrom = layout.newKey();
                SpatialIndexKey treeKeyTo = layout.newKey();
                initializeKeys( treeKeyFrom, treeKeyTo );
                treeKeyFrom.fromDerivedValue( Long.MIN_VALUE, range.min );
                treeKeyTo.fromDerivedValue( Long.MAX_VALUE, range.max + 1 );
                RawCursor<Hit<SpatialIndexKey,VALUE>,IOException> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo, IndexOrder.NONE );
                IndexProgressor hitProgressor = new NativeHitIndexProgressor<>( seeker, client, openSeekers );
                multiProgressor.initialize( descriptor, hitProgressor, query, IndexOrder.NONE, false );
            }
        }
        catch ( IllegalArgumentException e )
        {
            // Invalid query ranges will cause this state (eg. min>max)
            client.initialize( descriptor, IndexProgressor.EMPTY, query, IndexOrder.NONE, false );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    void startSeekForInitializedRange( IndexProgressor.NodeValueClient client, SpatialIndexKey treeKeyFrom, SpatialIndexKey treeKeyTo, IndexQuery[] query,
            IndexOrder indexOrder, boolean needFilter, boolean needsValues )
    {
        // Spatial does not support providing values
        assert !needsValues;

        if ( layout.compare( treeKeyFrom, treeKeyTo ) > 0 )
        {
            client.initialize( descriptor, IndexProgressor.EMPTY, query, IndexOrder.NONE, false );
            return;
        }
        try
        {
            RawCursor<Hit<SpatialIndexKey,VALUE>,IOException> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo, indexOrder );
            IndexProgressor hitProgressor = new NativeHitIndexProgressor<>( seeker, client, openSeekers );
            client.initialize( descriptor, hitProgressor, query, IndexOrder.NONE, false );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }

    private void initializeKeys( SpatialIndexKey treeKeyFrom, SpatialIndexKey treeKeyTo )
    {
        treeKeyFrom.initialize( Long.MIN_VALUE );
        treeKeyTo.initialize( Long.MAX_VALUE );
    }
}
