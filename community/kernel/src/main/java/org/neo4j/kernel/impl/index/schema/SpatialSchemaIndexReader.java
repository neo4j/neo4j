/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.util.Arrays;
import java.util.List;

import org.neo4j.cursor.RawCursor;
import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.GeometryRangePredicate;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.fusion.BridgingIndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

public class SpatialSchemaIndexReader<KEY extends SpatialSchemaKey, VALUE extends NativeSchemaValue> extends NativeSchemaIndexReader<KEY,VALUE>
{
    private final SpatialLayout spatial;
    private final SpaceFillingCurveConfiguration configuration;

    SpatialSchemaIndexReader( GBPTree<KEY,VALUE> tree, Layout<KEY,VALUE> layout, IndexSamplingConfig samplingConfig, IndexDescriptor descriptor,
            SpaceFillingCurveConfiguration configuration )
    {
        super( tree, layout, samplingConfig, descriptor );
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

        if ( indexOrder != IndexOrder.NONE )
        {
            throw new UnsupportedOperationException(
                    format( "Tried to query index with unsupported order %s. Supported orders for query %s are %s.", indexOrder, Arrays.toString( predicates ),
                            IndexOrder.NONE ) );
        }
    }

    @Override
    void initializeRangeForQuery( KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] predicates )
    {
        throw new UnsupportedOperationException( "Cannot initialize 1D range in multidimensional spatial index reader" );
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, IndexQuery... predicates )
    {
        validateQuery( indexOrder, predicates );
        IndexQuery predicate = predicates[0];
        switch ( predicate.type() )
        {
        case exists:
            startSeekForExists( cursor, predicate );
            break;
        case exact:
            startSeekForExact( cursor, ((ExactPredicate) predicate).value(), predicate );
            break;
        case rangeGeometric:
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

    private void startSeekForExists( IndexProgressor.NodeValueClient client, IndexQuery... predicates )
    {
        KEY treeKeyFrom = layout.newKey();
        KEY treeKeyTo = layout.newKey();
        treeKeyFrom.initAsLowest();
        treeKeyTo.initAsHighest();
        startSeekForInitializedRange( client, treeKeyFrom, treeKeyTo, predicates );
    }

    private void startSeekForExact( IndexProgressor.NodeValueClient client, Value value, IndexQuery... predicates )
    {
        KEY treeKeyFrom = layout.newKey();
        KEY treeKeyTo = layout.newKey();
        treeKeyFrom.from( Long.MIN_VALUE, value );
        treeKeyTo.from( Long.MAX_VALUE, value );
        startSeekForInitializedRange( client, treeKeyFrom, treeKeyTo, predicates );
    }

    private void startSeekForRange( IndexProgressor.NodeValueClient client, GeometryRangePredicate rangePredicate, IndexQuery[] query )
    {
        try
        {
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( client, descriptor.schema().getPropertyIds() );
            client.initialize( descriptor, multiProgressor, query );
            SpaceFillingCurve curve = spatial.getSpaceFillingCurve();
            Envelope completeEnvelope = SpatialCRSSchemaIndex.envelopeFromCRS( spatial.crs );
            double[] from = rangePredicate.from() == null ? completeEnvelope.getMin() : rangePredicate.from().coordinate();
            double[] to = rangePredicate.to() == null ? completeEnvelope.getMax() : rangePredicate.to().coordinate();
            Envelope envelope = new Envelope( from, to );
            List<SpaceFillingCurve.LongRange> ranges = curve.getTilesIntersectingEnvelope( envelope, configuration );
            for ( SpaceFillingCurve.LongRange range : ranges )
            {
                KEY treeKeyFrom = layout.newKey();
                KEY treeKeyTo = layout.newKey();
                treeKeyFrom.fromDerivedValue( Long.MIN_VALUE, range.min );
                treeKeyTo.fromDerivedValue( Long.MAX_VALUE, range.max + 1 );
                RawCursor<Hit<KEY,VALUE>,IOException> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo );
                IndexProgressor hitProgressor = new SpatialHitIndexProgressor<>( seeker, client, openSeekers );
                multiProgressor.initialize( descriptor, hitProgressor, query );
            }
        }
        catch ( IllegalArgumentException e )
        {
            // Invalid query ranges will cause this state (eg. min>max)
            client.initialize( descriptor, IndexProgressor.EMPTY, query );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    void startSeekForInitializedRange( IndexProgressor.NodeValueClient client, KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] query )
    {
        if ( layout.compare( treeKeyFrom, treeKeyTo ) > 0 )
        {
            client.initialize( descriptor, IndexProgressor.EMPTY, query );
            return;
        }
        try
        {
            RawCursor<Hit<KEY,VALUE>,IOException> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo );
            IndexProgressor hitProgressor = new SpatialHitIndexProgressor<>( seeker, client, openSeekers );
            client.initialize( descriptor, hitProgressor, query );
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
}
