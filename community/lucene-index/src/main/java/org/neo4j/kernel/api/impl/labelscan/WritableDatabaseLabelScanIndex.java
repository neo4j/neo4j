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

import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.WritableAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Writable implementation of Lucene label scan store index.
 */
public class WritableDatabaseLabelScanIndex extends WritableAbstractDatabaseIndex<LuceneLabelScanIndex>
        implements LabelScanIndex
{

    public WritableDatabaseLabelScanIndex( BitmapDocumentFormat format, PartitionedIndexStorage indexStorage )
    {
        super( new LuceneLabelScanIndex( indexStorage,
                new WritableIndexPartitionFactory( IndexWriterConfigs::standard ), format ) );
    }

    @Override
    public LabelScanReader getLabelScanReader()
    {
        return luceneIndex.getLabelScanReader();
    }

    @Override
    public LabelScanWriter getLabelScanWriter()
    {
        return luceneIndex.getLabelScanWriter( this );
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
