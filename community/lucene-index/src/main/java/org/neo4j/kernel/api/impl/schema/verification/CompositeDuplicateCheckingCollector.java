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

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema.OrderedPropertyValues;

public class CompositeDuplicateCheckingCollector extends DuplicateCheckingCollector
{
    private final int[] propertyKeyIds;
    private CompositeEntrySet actualValues;

    public CompositeDuplicateCheckingCollector( PropertyAccessor accessor, int[] propertyKeyIds )
    {
        super(accessor, -1);
        this.propertyKeyIds = propertyKeyIds;
        actualValues = new CompositeEntrySet();
    }

    @Override
    protected void doCollect( int doc ) throws IOException, KernelException, IndexEntryConflictException
    {
        Document document = reader.document( doc );
        long nodeId = LuceneDocumentStructure.getNodeId( document );
        Property[] properties = new Property[propertyKeyIds.length];
        Object[] values = new Object[propertyKeyIds.length];
        for ( int i = 0; i < properties.length; i++ )
        {
            properties[i] = accessor.getProperty( nodeId, propertyKeyIds[i] );
            values[i] = properties[i].value();
        }

        // We either have to find the first conflicting entry set element,
        // or append one for the property we just fetched:
        CompositeEntrySet current = actualValues;
        scan:
        do
        {
            for ( int i = 0; i < CompositeEntrySet.INCREMENT; i++ )
            {
                Object[] currentValues = current.values[i];

                if ( current.nodeId[i] == StatementConstants.NO_SUCH_NODE )
                {
                    current.values[i] = values;
                    current.nodeId[i] = nodeId;
                    if ( i == CompositeEntrySet.INCREMENT - 1 )
                    {
                        current.next = new CompositeEntrySet();
                    }
                    break scan;
                }
                else if ( propertyValuesEqual( properties, currentValues ) )
                {
                    throw new IndexEntryConflictException( current.nodeId[i], nodeId,
                            OrderedPropertyValues.ofUndefined( currentValues ) );
                }
            }
            current = current.next;
        }
        while ( current != null );
    }

    private boolean propertyValuesEqual( Property[] properties, Object[] values )
    {
        if ( properties.length != values.length )
        {
            return false;
        }
        for ( int i = 0; i < properties.length; i++ )
        {
            if ( !properties[i].valueEquals( values[i] ) )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean needsScores()
    {
        return false;
    }

    public void reset()
    {
        actualValues = new CompositeEntrySet();
    }

    /**
     * A small struct of arrays of nodeId + array of property values, with a next pointer.
     * Should exhibit fairly fast linear iteration, small memory overhead and dynamic growth.
     * <p>
     * NOTE: Must always call reset() before use!
     */
    private static class CompositeEntrySet
    {
        static final int INCREMENT = 10000;

        Object[][] values = new Object[INCREMENT][];
        long[] nodeId = new long[INCREMENT];
        CompositeEntrySet next;

        CompositeEntrySet()
        {
            Arrays.fill( nodeId, StatementConstants.NO_SUCH_NODE );
        }
    }
}
