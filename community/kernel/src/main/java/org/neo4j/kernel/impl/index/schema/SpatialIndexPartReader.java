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

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
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
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.fusion.BridgingIndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

public class SpatialIndexPartReader<VALUE extends NativeSchemaValue> extends NativeSchemaIndexReader<SpatialSchemaKey,VALUE>
{
    private final SpatialLayout spatial;
    private final SpaceFillingCurveConfiguration configuration;

    SpatialIndexPartReader( GBPTree<SpatialSchemaKey,VALUE> tree, Layout<SpatialSchemaKey,VALUE> layout,
            IndexSamplingConfig samplingConfig, SchemaIndexDescriptor descriptor,
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

        CapabilityValidator.validateQuery( SpatialIndexProvider.CAPABILITY, indexOrder, predicates );
    }

    @Override
    boolean initializeRangeForQuery( SpatialSchemaKey treeKeyFrom, SpatialSchemaKey treeKeyTo, IndexQuery[] predicates )
    {
        throw new UnsupportedOperationException( "Cannot initialize 1D range in multidimensional spatial index reader" );
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates )
    {
        NodeValueIterator nodeValueIterator = new NodeValueIterator();
        query( nodeValueIterator, IndexOrder.NONE, predicates );
        return nodeValueIterator;
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

    private void startSeekForExists( IndexProgressor.NodeValueClient client, IndexQuery... predicates )
    {
        SpatialSchemaKey treeKeyFrom = layout.newKey();
        SpatialSchemaKey treeKeyTo = layout.newKey();
        treeKeyFrom.initAsLowest();
        treeKeyTo.initAsHighest();
        startSeekForInitializedRange( client, treeKeyFrom, treeKeyTo, predicates, false );
    }

    private void startSeekForExact( IndexProgressor.NodeValueClient client, Value value, IndexQuery... predicates )
    {
        SpatialSchemaKey treeKeyFrom = layout.newKey();
        SpatialSchemaKey treeKeyTo = layout.newKey();
        treeKeyFrom.from( Long.MIN_VALUE, value );
        treeKeyTo.from( Long.MAX_VALUE, value );
        startSeekForInitializedRange( client, treeKeyFrom, treeKeyTo, predicates, false );
    }

    private void startSeekForRange( IndexProgressor.NodeValueClient client, GeometryRangePredicate rangePredicate, IndexQuery[] query )
    {
        try
        {
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( client, descriptor.schema().getPropertyIds() );
            client.initialize( descriptor, multiProgressor, query );
            SpaceFillingCurve curve = spatial.getSpaceFillingCurve();
            double[] from = rangePredicate.from() == null ? null : rangePredicate.from().coordinate();
            double[] to = rangePredicate.to() == null ? null : rangePredicate.to().coordinate();
            List<SpaceFillingCurve.LongRange> ranges = curve.getTilesIntersectingEnvelope( from, to, configuration );
            for ( SpaceFillingCurve.LongRange range : ranges )
            {
                SpatialSchemaKey treeKeyFrom = layout.newKey();
                SpatialSchemaKey treeKeyTo = layout.newKey();
                treeKeyFrom.fromDerivedValue( Long.MIN_VALUE, range.min );
                treeKeyTo.fromDerivedValue( Long.MAX_VALUE, range.max + 1 );
                RawCursor<Hit<SpatialSchemaKey,VALUE>,IOException> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo );
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
    void startSeekForInitializedRange( IndexProgressor.NodeValueClient client, SpatialSchemaKey treeKeyFrom,
            SpatialSchemaKey treeKeyTo, IndexQuery[] query, boolean needFilter )
    {
        if ( layout.compare( treeKeyFrom, treeKeyTo ) > 0 )
        {
            client.initialize( descriptor, IndexProgressor.EMPTY, query );
            return;
        }
        try
        {
            RawCursor<Hit<SpatialSchemaKey,VALUE>,IOException> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo );
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
