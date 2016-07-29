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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;

/**
 * A {@link LuceneIndexPopulatingUpdater} used for unique Lucene schema indexes.
 * Verifies uniqueness of added and changed values when closed using
 * {@link SchemaIndex#verifyUniqueness(PropertyAccessor, int, List)} method.
 */
public class UniqueLuceneIndexPopulatingUpdater extends LuceneIndexPopulatingUpdater
{
    private final int propertyKeyId;
    private final SchemaIndex luceneIndex;
    private final PropertyAccessor propertyAccessor;
    private final UniqueIndexSampler sampler;

    private final List<Object> updatedPropertyValues = new ArrayList<>();

    public UniqueLuceneIndexPopulatingUpdater( LuceneIndexWriter writer, int propertyKeyId,
            SchemaIndex luceneIndex, PropertyAccessor propertyAccessor, UniqueIndexSampler sampler )
    {
        super( writer );
        this.propertyKeyId = propertyKeyId;
        this.luceneIndex = luceneIndex;
        this.propertyAccessor = propertyAccessor;
        this.sampler = sampler;
    }

    @Override
    protected void added( NodePropertyUpdate update )
    {
        sampler.increment( 1 );
        updatedPropertyValues.add( update.getValueAfter() );
    }

    @Override
    protected void changed( NodePropertyUpdate update )
    {
        // sampler.increment( -1 ); // remove old vale
        // sampler.increment( 1 ); // add new value

        updatedPropertyValues.add( update.getValueAfter() );
    }

    @Override
    protected void removed( NodePropertyUpdate update )
    {
        sampler.increment( -1 );
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        luceneIndex.verifyUniqueness( propertyAccessor, propertyKeyId, updatedPropertyValues );
    }
}
