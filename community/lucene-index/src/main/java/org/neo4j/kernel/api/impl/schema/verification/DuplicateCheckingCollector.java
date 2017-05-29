/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema.verification;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SimpleCollector;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.values.Value;

public class DuplicateCheckingCollector extends SimpleCollector
{
    protected final PropertyAccessor accessor;
    private final int propertyKeyId;
    private EntrySet actualValues;
    protected LeafReader reader;

    public static DuplicateCheckingCollector forProperties( PropertyAccessor accessor, int[] propertyKeyIds )
    {
        return (propertyKeyIds.length == 1) ? new DuplicateCheckingCollector( accessor, propertyKeyIds[0] )
                                            : new CompositeDuplicateCheckingCollector( accessor, propertyKeyIds );
    }

    public DuplicateCheckingCollector( PropertyAccessor accessor, int propertyKeyId )
    {
        this.accessor = accessor;
        this.propertyKeyId = propertyKeyId;
        actualValues = new EntrySet();
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
        Value reference = accessor.getPropertyValue( nodeId, propertyKeyId );

        // We either have to find the first conflicting entry set element,
        // or append one for the property we just fetched:
        EntrySet currentEntrySet = actualValues;
        scan:
        do
        {
            for ( int i = 0; i < EntrySet.INCREMENT; i++ )
            {
                Value value = currentEntrySet.value[i];

                if ( currentEntrySet.nodeId[i] == StatementConstants.NO_SUCH_NODE )
                {
                    currentEntrySet.value[i] = reference;
                    currentEntrySet.nodeId[i] = nodeId;
                    if ( i == EntrySet.INCREMENT - 1 )
                    {
                        currentEntrySet.next = new EntrySet();
                    }
                    break scan;
                }
                else if ( reference.equals( value ) )
                {
                    throw new IndexEntryConflictException( currentEntrySet.nodeId[i], nodeId, value );
                }
            }
            currentEntrySet = currentEntrySet.next;
        }
        while ( currentEntrySet != null );
    }

    @Override
    protected void doSetNextReader( LeafReaderContext context ) throws IOException
    {
        this.reader = context.reader();
    }

    @Override
    public boolean needsScores()
    {
        return false;
    }

    public void reset()
    {
        actualValues = new EntrySet();
    }

    /**
     * A small struct of arrays of nodeId + property values, with a next pointer.
     * Should exhibit fairly fast linear iteration, small memory overhead and dynamic growth.
     * <p>
     * NOTE: Must always call reset() before use!
     */
    private static class EntrySet
    {
        static final int INCREMENT = 10000;

        Value[] value = new Value[INCREMENT];
        long[] nodeId = new long[INCREMENT];
        EntrySet next;

        EntrySet()
        {
            Arrays.fill( nodeId, StatementConstants.NO_SUCH_NODE );
        }
    }
}
