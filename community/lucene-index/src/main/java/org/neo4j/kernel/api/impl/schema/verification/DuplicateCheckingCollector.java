/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema.verification;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SimpleCollector;

import java.io.IOException;

import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.verification.DuplicateCheckStrategy.BucketsDuplicateCheckStrategy;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.values.storable.Value;

public class DuplicateCheckingCollector extends SimpleCollector
{
    protected final PropertyAccessor accessor;
    private final int propertyKeyId;
    protected LeafReader reader;
    DuplicateCheckStrategy duplicateCheckStrategy;

    static DuplicateCheckingCollector forProperties( PropertyAccessor accessor, int[] propertyKeyIds )
    {
        return (propertyKeyIds.length == 1) ? new DuplicateCheckingCollector( accessor, propertyKeyIds[0] )
                                            : new CompositeDuplicateCheckingCollector( accessor, propertyKeyIds );
    }

    DuplicateCheckingCollector( PropertyAccessor accessor, int propertyKeyId )
    {
        this.accessor = accessor;
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    public void collect( int doc ) throws IOException
    {
        try
        {
            doCollect( doc );
        }
        catch ( KernelException e )
        {
            throw new IllegalStateException( "Indexed node should exist and have the indexed property.", e );
        }
        catch ( IndexEntryConflictException e )
        {
            throw new IOException( e );
        }
    }

    protected void doCollect( int doc ) throws IOException, KernelException, IndexEntryConflictException
    {
        Document document = reader.document( doc );
        long nodeId = LuceneDocumentStructure.getNodeId( document );
        Value value = accessor.getPropertyValue( nodeId, propertyKeyId );
        duplicateCheckStrategy.checkForDuplicate( value, nodeId );
    }

    @Override
    protected void doSetNextReader( LeafReaderContext context )
    {
        this.reader = context.reader();
    }

    @Override
    public boolean needsScores()
    {
        return false;
    }

    /**
     * Initialise collector for unknown number of entries that are suspected to be duplicates.
     */
    public void init()
    {
        duplicateCheckStrategy = new BucketsDuplicateCheckStrategy();
    }

    /**
     * Initialize collector for some known and expected number of entries that are suspected to be duplicates.
     * @param expectedNumberOfEntries expected number entries
     */
    public void init( int expectedNumberOfEntries )
    {
        if ( useFastCheck( expectedNumberOfEntries ) )
        {
            duplicateCheckStrategy = new DuplicateCheckStrategy.MapDuplicateCheckStrategy( expectedNumberOfEntries );
        }
        else
        {
            duplicateCheckStrategy = new BucketsDuplicateCheckStrategy( expectedNumberOfEntries );
        }
    }

    private boolean useFastCheck( int expectedNumberOfEntries )
    {
        return expectedNumberOfEntries <= BucketsDuplicateCheckStrategy.BUCKET_STRATEGY_ENTRIES_THRESHOLD;
    }
}
