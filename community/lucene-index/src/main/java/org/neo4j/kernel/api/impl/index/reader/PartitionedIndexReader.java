/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.reader;

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.register.Register;
import org.neo4j.storageengine.api.schema.IndexReader;

public class PartitionedIndexReader implements IndexReader
{

    private List<PartitionSearcher> partitionSearchers;

    public PartitionedIndexReader( List<PartitionSearcher> partitionSearchers )
    {
        this.partitionSearchers = partitionSearchers;
    }

    @Override
    public PrimitiveLongIterator seek( Object value )
    {
        return null;
    }

    @Override
    public PrimitiveLongIterator rangeSeekByNumberInclusive( Number lower, Number upper )
    {
        return null;
    }

    @Override
    public PrimitiveLongIterator rangeSeekByString( String lower, boolean includeLower, String upper,
            boolean includeUpper )
    {
        return null;
    }

    @Override
    public PrimitiveLongIterator rangeSeekByPrefix( String prefix )
    {
        return null;
    }

    @Override
    public PrimitiveLongIterator scan()
    {
        return null;
    }

    @Override
    public int countIndexedNodes( long nodeId, Object propertyValue )
    {
        return 0;
    }

    @Override
    public long sampleIndex( Register.DoubleLong.Out result ) throws IndexNotFoundKernelException
    {
        return 0;
    }

    @Override
    public void verifyDeferredConstraints( Object accessor, int propertyKeyId )
            throws Exception
    {

    }

    @Override
    public void verifyDeferredConstraints( Object accessor, int propertyKeyId,
            List<Object> updatedPropertyValues ) throws Exception
    {

    }

    @Override
    public long getMaxDoc()
    {
        return 0;
    }

    @Override
    public Iterator<Document> getAllDocsIterator()
    {
        return null;
    }

    @Override
    public void close()
    {
        try
        {
            IOUtils.closeAll( partitionSearchers );
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }
}
