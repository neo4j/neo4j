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
package org.neo4j.kernel.api.impl.labelscan;

import org.neo4j.kernel.api.impl.index.ReadOnlyAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Read only label scan store index
 */
public class ReadOnlyDatabaseLabelScanIndex extends ReadOnlyAbstractDatabaseIndex<LuceneLabelScanIndex> implements
        LabelScanIndex
{

    public ReadOnlyDatabaseLabelScanIndex( BitmapDocumentFormat format, PartitionedIndexStorage indexStorage )
    {
        super( new LuceneLabelScanIndex( indexStorage, new ReadOnlyIndexPartitionFactory(), format ) );
    }

    @Override
    public LabelScanReader getLabelScanReader()
    {
        return luceneIndex.getLabelScanReader();
    }

    @Override
    public LabelScanWriter getLabelScanWriter()
    {
        throw new UnsupportedOperationException( "Can't create index writer in read only mode." );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AllEntriesLabelScanReader allNodeLabelRanges()
    {
        return luceneIndex.allNodeLabelRanges();
    }

}

