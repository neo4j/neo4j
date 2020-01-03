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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.queryparser.classic.ParseException;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;

public abstract class FulltextIndexReader implements IndexReader
{
    /**
     * Queires the fulltext index with the given lucene-syntax query
     *
     * @param query the lucene query
     * @return A {@link ScoreEntityIterator} over the results
     */
    public abstract ScoreEntityIterator query( String query ) throws ParseException;

    @Override
    public IndexSampler createSampler()
    {
        return IndexSampler.EMPTY;
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        throw new IndexNotApplicableKernelException( "Fulltext indexes does not support IndexQuery queries" );
    }

    @Override
    public void query( IndexProgressor.NodeValueClient client, IndexOrder indexOrder, boolean needsValues, IndexQuery... query )
            throws IndexNotApplicableKernelException
    {
        throw new IndexNotApplicableKernelException( "Fulltext indexes does not support IndexQuery queries" );
    }

    @Override
    public void distinctValues( IndexProgressor.NodeValueClient client, NodePropertyAccessor propertyAccessor, boolean needsValues )
    {
        throw new UnsupportedOperationException( "Fulltext indexes does not support distinctValues queries" );
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }
}
