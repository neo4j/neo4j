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
package org.neo4j.kernel.impl.api.index.persson;

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.SCIndex;
import org.neo4j.index.SCIndexDescription;
import org.neo4j.index.SCInserter;
import org.neo4j.index.SCResult;
import org.neo4j.index.Seeker;
import org.neo4j.index.btree.Index;
import org.neo4j.index.btree.RangeSeeker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.neo4j.index.SCIndex.indexFileName;
import static org.neo4j.index.SCIndex.metaFileName;
import static org.neo4j.index.btree.RangePredicate.equalTo;

public class NativeLabelScanStore implements LabelScanStore
{
    private final SCIndex index;

    public NativeLabelScanStore( PageCache pageCache, File storeDir ) throws IOException
    {
        String name = "labelscan.db";
        File indexFile = new File( storeDir, indexFileName( name ) );
        File metaFile = new File( storeDir, metaFileName( name ));
        this.index = new Index( pageCache, indexFile, metaFile,
                new SCIndexDescription( "", "", "", Direction.BOTH, "", null ), pageCache.pageSize() );
    }

    @Override
    public LabelScanReader newReader()
    {
        return new LabelScanReader()
        {
            @Override
            public void close()
            {
                // No
            }

            @Override
            public PrimitiveLongIterator nodesWithLabel( int labelId )
            {
                Seeker seeker = new RangeSeeker( equalTo( labelId, 0L ), equalTo( labelId, 0L ) );
                final ArrayList<SCResult> resultList = new ArrayList<>();
                try
                {
                    index.seek( seeker, resultList );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
                return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
                {
                    Iterator<SCResult> iterator = resultList.iterator();
                    @Override
                    protected boolean fetchNext()
                    {
                        if ( !iterator.hasNext() )
                        {
                            return false;
                        }
                        final long nodeId = iterator.next().getValue().getRelId();
                        return next( nodeId );
                    }
                };
            }

            @Override
            public PrimitiveLongIterator labelsForNode( long nodeId )
            {
                throw new UnsupportedOperationException( "Use your db..." );
            }
        };
    }

    @Override
    public LabelScanWriter newWriter()
    {
        final SCInserter inserter;
        try
        {
            inserter = index.inserter();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return new LabelScanWriter()
        {
            @Override
            public void write( NodeLabelUpdate update ) throws IOException
            {
                final long[] labelsAfter = update.getLabelsAfter();
                final long[] labelsBefore = update.getLabelsBefore();
                final long[] toAdd = ArrayUtils.removeElements( labelsAfter, labelsBefore );
                final long nodeId = update.getNodeId();
                for ( long labelId : toAdd )
                {
                    inserter.insert( new long[]{labelId, 0L}, new long[]{nodeId, 0L} ); //TODO reuse
                }
            }

            @Override
            public void close() throws IOException
            {
                inserter.close();
            }
        };
    }

    @Override
    public void force() throws UnderlyingStorageException
    {

    }

    @Override
    public AllEntriesLabelScanReader allNodeLabelRanges()
    {
        return null;
    }

    @Override
    public ResourceIterator<File> snapshotStoreFiles() throws IOException
    {
        return null;
    }

    @Override
    public void init() throws IOException
    {

    }

    @Override
    public void start() throws IOException
    {

    }

    @Override
    public void stop() throws IOException
    {

    }

    @Override
    public void shutdown() throws IOException
    {
        index.close();
    }
}
