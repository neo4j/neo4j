package org.neo4j.index.internal.gbptree;

import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.io.pagecache.PageCursor;

class OffloadStore<KEY,VALUE>
{
    private final Layout<KEY,VALUE> layout;
    private final Supplier<PageCursor> pcFactory;

    OffloadStore( Layout<KEY,VALUE> layout )
    {
        this.layout = layout;
        this.pcFactory = () -> null;
    }

    void readKey( long offloadId, KEY into ) throws IOException
    {
        try ( PageCursor cursor = pcFactory.get() )
        {
            do
            {
                placeCursorAtOffloadId( cursor, offloadId );

                int keySize = cursor.getInt();
                int valueSize = cursor.getInt();
                layout.readKey( cursor, into, keySize );
            }
            while ( cursor.shouldRetry() );
        }
    }

    void readKeyValue( long offloadId, KEY key, VALUE value ) throws IOException
    {
        try ( PageCursor cursor = pcFactory.get() )
        {
            do
            {
                placeCursorAtOffloadId( cursor, offloadId );

                int keySize = cursor.getInt();
                int valueSize = cursor.getInt();
                layout.readKey( cursor, key, keySize );
                layout.readValue( cursor, value, valueSize );
            }
            while ( cursor.shouldRetry() );
        }
    }

    void readValue( long offloadId, VALUE into ) throws IOException
    {
        try ( PageCursor cursor = pcFactory.get() )
        {
            do
            {
                placeCursorAtOffloadId( cursor, offloadId );

                int keySize = cursor.getInt();
                int valueSize = cursor.getInt();
                cursor.setOffset( cursor.getOffset() + keySize );
                layout.readValue( cursor, into, valueSize );
            }
            while ( cursor.shouldRetry() );
        }
    }

    long writeKey( KEY key )
    {
        try ( PageCursor cursor = pcFactory.get() )
        {
            int keySize = layout.keySize( key );
            long newId = acquireNewId( keySize );
            placeCursorAtOffloadId( cursor, newId );

            putKeyValueSize( cursor, keySize, 0 );
            layout.writeKey( cursor, key );
            return newId;
        }
    }

    long writeKeyValue( KEY key, VALUE value )
    {
        try ( PageCursor cursor = pcFactory.get() )
        {
            int keySize = layout.keySize( key );
            int valueSize = layout.valueSize( value );
            long newId = acquireNewId( keySize + valueSize );
            placeCursorAtOffloadId( cursor, newId );

            putKeyValueSize( cursor, keySize, valueSize );
            layout.writeKey( cursor, key );
            layout.writeValue( cursor, value );
            return newId;
        }
    }

    void free( long offloadId )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    private void putKeyValueSize( PageCursor cursor, int keySize, int valueSize )
    {
        cursor.putInt( keySize );
        cursor.putInt( valueSize );
    }

    private long acquireNewId( int keySize )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    private void placeCursorAtOffloadId( PageCursor cursor, long offloadId )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }
}
