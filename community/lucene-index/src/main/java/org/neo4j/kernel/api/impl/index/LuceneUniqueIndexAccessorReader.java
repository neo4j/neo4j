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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.search.IndexSearcher;

import java.io.Closeable;

import org.neo4j.helpers.CancellationRequest;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;

import static org.neo4j.register.Register.DoubleLong;


class LuceneUniqueIndexAccessorReader extends LuceneIndexAccessorReader
{
    LuceneUniqueIndexAccessorReader( IndexSearcher searcher, LuceneDocumentStructure documentLogic, Closeable onClose,
                                     CancellationRequest cancellation )
    {
        super( searcher, documentLogic, onClose, cancellation, -1 /* unused */ );
    }

    /**
     * Implementation note:
     * re-uses the {@link UniqueIndexSampler} in order to know that we have the same semantics as in
     * {@link DeferredConstraintVerificationUniqueLuceneIndexPopulator population}.
     */
    @Override
    public long sampleIndex( DoubleLong.Out result ) throws IndexNotFoundKernelException
    {
        UniqueIndexSampler sampler = new UniqueIndexSampler();
        sampler.increment( luceneIndexReader().numDocs() );
        checkCancellation();
        return sampler.result( result );
    }
}
