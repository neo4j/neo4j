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
package org.neo4j.kernel.api.impl.labelscan.storestrategy;

import org.apache.lucene.search.IndexSearcher;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.impl.index.LuceneAllDocumentsReader;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;

public interface LabelScanStorageStrategy
{
    PrimitiveLongIterator nodesWithLabel( IndexSearcher searcher, int labelId );

    PrimitiveLongIterator nodesWithAnyOfLabels( IndexSearcher indexSearcher, int[] labelIds );

    PrimitiveLongIterator nodesWithAllLabels( IndexSearcher indexSearcher, int[] labelIds );

    AllEntriesLabelScanReader newNodeLabelReader( LuceneAllDocumentsReader allDocumentsReader );

    PrimitiveLongIterator labelsForNode( IndexSearcher searcher, long nodeId );

}
