package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.fulltext.lucene.ReadOnlyFulltext;
import org.neo4j.kernel.api.impl.fulltext.lucene.WritableFulltext;
import org.neo4j.kernel.api.impl.schema.reader.LuceneAllEntriesIndexAccessorReader;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;

public class FulltextIndexAccessor implements IndexAccessor
{
    private WritableFulltext luceneFulltext;

    public FulltextIndexAccessor( WritableFulltext luceneFulltext )
    {
        this.luceneFulltext = luceneFulltext;
    }

    @Override
    public void drop() throws IOException
    {
        luceneFulltext.drop();
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        throw new UnsupportedOperationException( "Not implemented" );
    }

    @Override
    public void force() throws IOException
    {
        luceneFulltext.markAsOnline();
        luceneFulltext.maybeRefreshBlocking();
    }

    @Override
    public void refresh() throws IOException
    {
        luceneFulltext.maybeRefreshBlocking();
    }

    @Override
    public void close() throws IOException
    {
        luceneFulltext.close();
    }

    @Override
    public IndexReader newReader()
    {
        System.out.println( "Tried to get reader" );
        return IndexReader.EMPTY;
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return new LuceneAllEntriesIndexAccessorReader( luceneFulltext.allDocumentsReader() );
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        throw new UnsupportedOperationException( "Not implemented" );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
    {
        //Sure whatever
    }

    public PrimitiveLongIterator query( String query ) throws IOException
    {
        try ( ReadOnlyFulltext indexReader = luceneFulltext.getIndexReader() )
        {
            return indexReader.query( Collections.singleton( query ), false );
        }
    }
}
