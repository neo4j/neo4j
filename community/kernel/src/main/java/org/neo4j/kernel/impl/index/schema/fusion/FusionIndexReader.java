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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.Arrays;

import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.BridgingIndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

class FusionIndexReader extends FusionIndexBase<IndexReader> implements IndexReader
{
    private final IndexDescriptor descriptor;

    FusionIndexReader( SlotSelector slotSelector, LazyInstanceSelector<IndexReader> instanceSelector, IndexDescriptor descriptor )
    {
        super( slotSelector, instanceSelector );
        this.descriptor = descriptor;
    }

    @Override
    public void close()
    {
        instanceSelector.close( Resource::close );
    }

    @Override
    public long countIndexedEntities( long entityId, PageCursorTracer cursorTracer, int[] propertyKeyIds, Value... propertyValues )
    {
        final IndexReader indexReader = instanceSelector.select( slotSelector.selectSlot( propertyValues, CATEGORY_OF ) );
        return indexReader.countIndexedEntities( entityId, cursorTracer, propertyKeyIds, propertyValues );
    }

    @Override
    public IndexSampler createSampler()
    {
        return new FusionIndexSampler( instanceSelector.transform( IndexReader::createSampler ) );
    }

    @Override
    public void query( QueryContext context, IndexProgressor.EntityValueClient cursor, IndexQueryConstraints constraints,
            IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        IndexSlot slot = slotSelector.selectSlot( predicates, IndexQuery::valueCategory );
        if ( slot != null )
        {
            instanceSelector.select( slot ).query( context, cursor, constraints, predicates );
        }
        else
        {
            if ( constraints.isOrdered() )
            {
                throw new UnsupportedOperationException(
                        format( "Tried to query index with unsupported order %s. Supported orders for query %s are %s.",
                                constraints.order(), Arrays.toString( predicates ), IndexOrder.NONE ) );
            }
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( cursor, descriptor.schema().getPropertyIds() );
            cursor.initialize( descriptor, multiProgressor, predicates, constraints, false );
            try
            {
                instanceSelector.forAll( reader ->
                {
                    try
                    {
                        reader.query( context, multiProgressor, constraints, predicates );
                    }
                    catch ( IndexNotApplicableKernelException e )
                    {
                        throw new InnerException( e );
                    }
                } );
            }
            catch ( InnerException e )
            {
                throw e.getCause();
            }
        }
    }

    private static final class InnerException extends RuntimeException
    {
        private InnerException( IndexNotApplicableKernelException e )
        {
            super( e );
        }

        @Override
        public synchronized IndexNotApplicableKernelException getCause()
        {
            return (IndexNotApplicableKernelException) super.getCause();
        }
    }
}
