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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Node id iterator used during index population when we go over node ids indexed in label scan store.
 * Before each iteration check if data in current reader can be expired because of concurrent updates that came from
 * other transactions.
 * In that case will reopen corresponding lucene reader and will reposition itself into the same position of matched
 * node ids.
 */
class LabelScanViewIdIterator implements PrimitiveLongResourceIterator
{
    private LabelScanViewNodeStoreScan labelScanViewNodeStoreScan;
    private LabelScanReader labelScanReader;
    private PrimitiveLongIterator idIterator;
    private LabelScanStore labelScanStore;
    private int[] labelIds;
    private long currentId = -1;

    LabelScanViewIdIterator( LabelScanViewNodeStoreScan labelScanViewNodeStoreScan,
            LabelScanStore labelScanStore, int[] labelIds )
    {
        this.labelScanViewNodeStoreScan = labelScanViewNodeStoreScan;
        this.labelScanStore = labelScanStore;
        this.labelIds = labelIds;
        idIterator = openIdIterator( labelScanStore, labelIds );
    }

    @Override
    public void close()
    {
        labelScanReader.close();
    }

    @Override
    public boolean hasNext()
    {
        checkNodeIdIterator();
        return idIterator.hasNext();
    }

    @Override
    public long next()
    {
        currentId = idIterator.next();
        return currentId;
    }

    private void checkNodeIdIterator()
    {
        if ( labelScanViewNodeStoreScan.isOutdated() )
        {
            close();
            idIterator = openIdIterator( labelScanStore, labelIds );
            labelScanViewNodeStoreScan.clearOutdatedFlag();
        }
    }

    private PrimitiveLongIterator openIdIterator( LabelScanStore labelScanStore, int[] labelIds )
    {
        labelScanReader = labelScanStore.newReader();
        return new LabeledNodesIdIterator( labelScanReader.nodesWithAnyOfLabels( labelIds ), currentId );
    }

    private class LabeledNodesIdIterator extends PrimitiveLongCollections.PrimitiveLongFilteringIterator
    {
        private long observedId;

        LabeledNodesIdIterator( PrimitiveLongIterator source, long observedId )
        {
            super( source );
            this.observedId = observedId;
        }

        @Override
        public boolean test( long testId )
        {
            return this.observedId < testId;
        }
    }
}
