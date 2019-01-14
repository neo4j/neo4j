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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * {@link LabelScanReader} for reading data from {@link NativeLabelScanStore}.
 * Each {@link PrimitiveLongIterator} returned from each of the methods is backed by {@link RawCursor}
 * directly from {@link GBPTree#seek(Object, Object)}.
 * <p>
 * The returned {@link PrimitiveLongIterator} aren't closable so the cursors retrieved are managed
 * inside of this reader and closed between each new query and on {@link #close()}.
 */
class NativeLabelScanReader implements LabelScanReader
{
    /**
     * {@link Index} which is queried when calling the methods below.
     */
    private final GBPTree<LabelScanKey,LabelScanValue> index;

    /**
     * Currently open {@link RawCursor} from query methods below. Open cursors are closed when calling
     * new query methods or when {@link #close() closing} this reader.
     */
    private final Set<RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>> openCursors;

    NativeLabelScanReader( GBPTree<LabelScanKey,LabelScanValue> index )
    {
        this.index = index;
        this.openCursors = new HashSet<>();
    }

    /**
     * Closes all currently open {@link RawCursor cursors} from last query method call.
     */
    @Override
    public void close()
    {
        try
        {
            ensureOpenCursorsClosed();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public PrimitiveLongResourceIterator nodesWithLabel( int labelId )
    {
        RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor;
        try
        {
            cursor = seekerForLabel( labelId );
            openCursors.add( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }

        return new LabelScanValueIterator( cursor, openCursors );
    }

    @Override
    public PrimitiveLongResourceIterator nodesWithAnyOfLabels( int... labelIds )
    {
        List<PrimitiveLongResourceIterator> iterators = iteratorsForLabels( labelIds );
        return new CompositeLabelScanValueIterator( iterators, false );
    }

    @Override
    public PrimitiveLongResourceIterator nodesWithAllLabels( int... labelIds )
    {
        List<PrimitiveLongResourceIterator> iterators = iteratorsForLabels( labelIds );
        return new CompositeLabelScanValueIterator( iterators, true );
    }

    @Override
    public void nodesWithLabel( IndexProgressor.NodeLabelClient client, int labelId )
    {
        RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor;
        try
        {
            cursor = seekerForLabel( labelId );
            openCursors.add( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }

        client.scan( new LabelScanValueIndexProgressor( cursor, openCursors, client ), false, labelId );
    }

    private List<PrimitiveLongResourceIterator> iteratorsForLabels( int[] labelIds )
    {
        List<PrimitiveLongResourceIterator> iterators = new ArrayList<>();
        try
        {
            for ( int labelId : labelIds )
            {
                RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor = seekerForLabel( labelId );
                openCursors.add( cursor );
                iterators.add( new LabelScanValueIterator( cursor, openCursors ) );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return iterators;
    }

    private RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> seekerForLabel( int labelId ) throws IOException
    {
        LabelScanKey from = new LabelScanKey( labelId, 0 );
        LabelScanKey to = new LabelScanKey( labelId, Long.MAX_VALUE );
        return index.seek( from, to );
    }

    private void ensureOpenCursorsClosed() throws IOException
    {
        for ( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor : openCursors )
        {
            cursor.close();
        }
        openCursors.clear();
    }
}
