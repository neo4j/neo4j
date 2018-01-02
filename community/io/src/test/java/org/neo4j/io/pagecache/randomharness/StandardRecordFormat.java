/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.io.pagecache.randomharness;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;

public class StandardRecordFormat extends RecordFormat
{
    private static final Charset CHARSET = Charset.forName( "UTF-8" );

    @Override
    public int getRecordSize()
    {
        return 16;
    }

    @Override
    public Record createRecord( File file, int recordId )
    {
        return new StandardRecord( file, recordId );
    }

    @Override
    public Record readRecord( PageCursor cursor ) throws IOException
    {
        int offset = cursor.getOffset();
        byte t;
        byte f;
        short f1;
        int r;
        long f2;
        do
        {
            cursor.setOffset( offset );
            t = cursor.getByte();
            f = cursor.getByte();
            f1 = cursor.getShort();
            r = cursor.getInt();
            f2 = cursor.getLong();
        }
        while ( cursor.shouldRetry() );
        return new StandardRecord( t, f, f1, r, f2 );
    }

    @Override
    public Record zeroRecord()
    {
        byte z = MuninnPageCache.ZERO_BYTE;
        short sz = (short) ((z << 8) + z);
        int iz = (sz << 16) + sz;
        long lz = (((long) iz) << 32) + iz;
        return new StandardRecord( z, z, sz, iz, lz );
    }

    @Override
    public void write( Record record, PageCursor cursor )
    {
        StandardRecord r = (StandardRecord) record;
        cursor.putByte( r.type );
        byte[] pathBytes = r.file.getPath().getBytes( CHARSET );
        cursor.putByte( pathBytes[pathBytes.length - 1] );
        cursor.putShort( r.fill1 );
        cursor.putInt( r.recordId );
        cursor.putLong( r.fill2 );
    }

    static final class StandardRecord implements Record
    {
        final byte type;
        final File file;
        final int recordId;
        final short fill1;
        final long fill2;

        public StandardRecord( File file, int recordId )
        {
            this.type = 42;
            this.file = file;
            this.recordId = recordId;
            int fileHash = file.hashCode();

            int a = xorshift( fileHash ^ xorshift( recordId ) );
            int b = xorshift( a );
            int c = xorshift( b );
            long d = b;
            d = d << 32;
            d += c;
            fill1 = (short) a;
            fill2 = d;
        }

        public StandardRecord( byte type, byte fileName, short fill1, int recordId, long fill2 )
        {
            this.type = type;
            this.file = fileName == 0? null : new File( new String( new byte[] {fileName} ) );
            this.fill1 = fill1;
            this.recordId = recordId;
            this.fill2 = fill2;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            StandardRecord record = (StandardRecord) o;

            return type == record.type
                   && recordId == record.recordId
                   && fill1 == record.fill1
                   && fill2 == record.fill2
                   && filesEqual( record );

        }

        private boolean filesEqual( StandardRecord record )
        {
            if ( file == record.file )
            {
                return true;
            }
            if ( file == null || record.file == null )
            {
                return false;
            }
            // We only look at the last letter of the path, because that's all that we can store in the record.
            byte[] thisPath = file.getPath().getBytes( CHARSET );
            byte[] thatPath = record.file.getPath().getBytes( CHARSET );
            return thisPath[thisPath.length - 1] == thatPath[thatPath.length - 1];
        }

        @Override
        public int hashCode()
        {
            int result = (int) type;
            result = 31 * result + (file != null ? file.hashCode() : 0);
            result = 31 * result + recordId;
            result = 31 * result + (int) fill1;
            result = 31 * result + (int) (fill2 ^ (fill2 >>> 32));
            return result;
        }

        private static int xorshift( int x )
        {
            x ^= (x << 6);
            x ^= (x >>> 21);
            return x ^ (x << 7);
        }

        @Override
        public String toString()
        {
            return format( type, file, recordId, fill1, fill2 );
        }

        public String format( byte type, File file, int recordId, short fill1, long fill2 )
        {
            return String.format(
                    "Record%s[file=%s, recordId=%s; %04x %016x]",
                    type, file, recordId, fill1, fill2 );
        }
    }
}
