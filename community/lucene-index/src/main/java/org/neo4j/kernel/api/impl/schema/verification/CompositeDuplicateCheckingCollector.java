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
package org.neo4j.kernel.api.impl.schema.verification;

import org.apache.lucene.document.Document;

import java.io.IOException;

import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Value;

import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

public class CompositeDuplicateCheckingCollector extends DuplicateCheckingCollector
{
    private final int[] propertyKeyIds;

    CompositeDuplicateCheckingCollector( NodePropertyAccessor accessor, int[] propertyKeyIds )
    {
        super( accessor, StatementConstants.NO_SUCH_PROPERTY_KEY );
        this.propertyKeyIds = propertyKeyIds;
    }

    @Override
    protected void doCollect( int doc ) throws IOException, KernelException
    {
        Document document = reader.document( doc );
        long nodeId = LuceneDocumentStructure.getNodeId( document );
        Value[] values = new Value[propertyKeyIds.length];
        for ( int i = 0; i < values.length; i++ )
        {
            values[i] = accessor.getNodePropertyValue( nodeId, propertyKeyIds[i], NULL );
        }
        duplicateCheckStrategy.checkForDuplicate( values, nodeId );
    }
}
