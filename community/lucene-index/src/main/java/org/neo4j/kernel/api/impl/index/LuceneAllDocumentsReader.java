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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;

import java.util.Iterator;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;

public class LuceneAllDocumentsReader implements BoundedIterable<Document>
{
    private LabelScanReader labelScanReader;
    private IndexReader indexReader;

    public LuceneAllDocumentsReader( IndexReader indexReader )
    {
        this.indexReader = indexReader;
    }

    public LuceneAllDocumentsReader( LabelScanReader labelScanReader )
    {
        this.labelScanReader = labelScanReader;
    }

    @Override
    public long maxCount()
    {
        return labelScanReader != null ? labelScanReader.getMaxDoc() : indexReader.getMaxDoc();
    }

    @Override
    public Iterator<Document> iterator()
    {
        return labelScanReader != null ? labelScanReader.getAllDocsIterator() : indexReader.getAllDocsIterator();
    }

    @Override
    public void close()
    {
        IOUtils.closeAllSilently( labelScanReader, labelScanReader );
    }
}
