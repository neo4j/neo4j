/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;

import static org.neo4j.register.Register.DoubleLong;


class LuceneUniqueIndexAccessorReader extends LuceneIndexAccessorReader
{
    private final IndexSearcher searcher;

    LuceneUniqueIndexAccessorReader( IndexSearcher searcher, LuceneDocumentStructure documentLogic, Closeable onClose )
    {
        super( searcher, documentLogic, onClose, -1 /* unused */ );
        this.searcher = searcher;
    }

    @Override
    public long sampleIndex( DoubleLong.Out result )
    {
        UniqueIndexSampler sampler = new UniqueIndexSampler();
        sampler.increment( searcher.getIndexReader().numDocs() );
        return sampler.result( result );
    }
}
