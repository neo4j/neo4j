package org.neo4j.io.pagecache.impl.common;

public interface Page
{
    byte getByte( int offset );

    long getLong( int offset );
    void putLong( long value, int offset );

    int getInt( int currentOffset );
    void putInt( int value, int offset );

    void getBytes( byte[] data, int offset );
    void putBytes( byte[] data, int offset );

    void putByte( byte value, int offset );
}
