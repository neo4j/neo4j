/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema.populator;

import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;

/**
 * A {@link LuceneIndexPopulatingUpdater} used for non-unique Lucene schema indexes.
 */
public class NonUniqueLuceneIndexPopulatingUpdater extends LuceneIndexPopulatingUpdater
{
    private final NonUniqueIndexSampler sampler;

    public NonUniqueLuceneIndexPopulatingUpdater( LuceneIndexWriter writer, NonUniqueIndexSampler sampler )
    {
        super( writer );
        this.sampler = sampler;
    }

    @Override
    protected void added( NodePropertyUpdate update )
    {
        String encodedValue = LuceneDocumentStructure.encodedStringValue( update.getValueAfter() );
        sampler.include( encodedValue );
    }

    @Override
    protected void changed( NodePropertyUpdate update )
    {
        String encodedValueBefore = LuceneDocumentStructure.encodedStringValue( update.getValueBefore() );
        sampler.exclude( encodedValueBefore );

        String encodedValueAfter = LuceneDocumentStructure.encodedStringValue( update.getValueAfter() );
        sampler.include( encodedValueAfter );
    }

    @Override
    protected void removed( NodePropertyUpdate update )
    {
        String removedValue = LuceneDocumentStructure.encodedStringValue( update.getValueBefore() );
        sampler.exclude( removedValue );
    }

    @Override
    public void close()
    {
    }
}
