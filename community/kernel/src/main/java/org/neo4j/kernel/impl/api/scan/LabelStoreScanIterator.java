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
package org.neo4j.kernel.impl.api.scan;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.storageengine.api.LabelItem;
import org.neo4j.storageengine.api.NodeItem;

import static java.util.Arrays.copyOf;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

public class LabelStoreScanIterator extends PrefetchingResourceIterator<NodeLabelUpdate>
{
    private long current;
    private final KernelTransaction tx;
    private final Statement statement;
    private final PrimitiveLongIterator nodeIds;
    private final ReadOperations readOperations;
    private final long[] tempArray = new long[10];

    public LabelStoreScanIterator( KernelTransaction tx )
    {
        this.tx = tx;
        this.statement = tx.acquireStatement();
        this.readOperations = statement.readOperations();
        this.nodeIds = readOperations.nodesGetAll();
    }

    @Override
    protected NodeLabelUpdate fetchNextOrNull()
    {
        while ( nodeIds.hasNext() )
        {
            long nodeId = nodeIds.next();
            try ( Cursor<NodeItem> nodeCursor = readOperations.nodeCursor( nodeId ) )
            {
                if ( nodeCursor.next() )
                {
                    Cursor<LabelItem> labelCursor = nodeCursor.get().labels();
                    long[] labels = allLabels( labelCursor );
                    if ( labels.length > 0 )
                    {
                        return NodeLabelUpdate.labelChanges( nodeId, EMPTY_LONG_ARRAY, labels );
                    }
                }
            }
        }
        return null;
    }

    private long[] allLabels( Cursor<LabelItem> labelCursor )
    {
        int index = 0;
        while ( labelCursor.next() )
        {
            tempArray[index++] = labelCursor.get().getAsInt();
        }
        return index == 0 ? EMPTY_LONG_ARRAY : copyOf( tempArray, index );
    }

    @Override
    public void close()
    {
        statement.close();
        try
        {
            tx.close();
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }
}
