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
package org.neo4j.kernel.impl.coreapi.internal;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.io.IOUtils;

public class NodeLabelPropertyIterator extends PrefetchingNodeResourceIterator
{
    private final Read read;
    private final NodeLabelIndexCursor nodeLabelCursor;
    private final NodeCursor nodeCursor;
    private final PropertyCursor propertyCursor;
    private final IndexQuery[] queries;

    public NodeLabelPropertyIterator(
            Read read,
            NodeLabelIndexCursor nodeLabelCursor,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            NodeFactory nodeFactory,
            IndexQuery... queries )
    {
        super( nodeFactory );
        this.read = read;
        this.nodeLabelCursor = nodeLabelCursor;
        this.nodeCursor = nodeCursor;
        this.propertyCursor = propertyCursor;
        this.queries = queries;
    }

    @Override
    protected long fetchNext()
    {
        boolean hasNext;
        do
        {
            hasNext = nodeLabelCursor.next();

        } while ( hasNext && !hasPropertiesWithValues() );

        if ( hasNext )
        {
            return nodeLabelCursor.nodeReference();
        }
        else
        {
            close();
            return NO_ID;
        }
    }

    @Override
    void closeResources()
    {
        IOUtils.closeAllSilently( nodeLabelCursor, nodeCursor, propertyCursor );
    }

    private boolean hasPropertiesWithValues()
    {
        int targetCount = queries.length;
        read.singleNode( nodeLabelCursor.nodeReference(), nodeCursor );
        if ( nodeCursor.next() )
        {
            nodeCursor.properties( propertyCursor );
            while ( propertyCursor.next() )
            {
                for ( IndexQuery query : queries )
                {
                    if ( propertyCursor.propertyKey() == query.propertyKeyId() )
                    {
                        if ( query.acceptsValueAt( propertyCursor ) )
                        {
                            targetCount--;
                            if ( targetCount == 0 )
                            {
                                return true;
                            }
                        }
                        else
                        {
                            return false;
                        }
                    }
                }
            }
        }
        return false;
    }
}
