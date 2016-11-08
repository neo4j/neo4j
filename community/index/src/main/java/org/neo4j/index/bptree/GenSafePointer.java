package org.neo4j.index.bptree;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Provides (mostly) static methods for getting and manipulating GSP (gen-safe pointer) data.
 * All interaction is made using a {@link PageCursor}. These methods are about a single GSP,
 * whereas the normal use case of a GSP is in pairs (GSPP).
 *
 * A GSP consists of [generation,pointer,checksum] where checksum is updated
 */
public class GenSafePointer
{
    /**
     * Data for a GSP, i.e. generation and pointer. Checksum is generated from those two fields and
     * so isn't a field in this struct - ahem class. The reason this class exists is that we, when reading,
     * want to read two fields and a checksum and match the two fields with the checksum. This class
     * is designed to be mutable and should be reused in as many places as possible.
     */
    public static class GSP
    {
        public long generation; // unsigned int
        public long pointer;

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (generation ^ (generation >>> 32));
            result = prime * result + (int) (pointer ^ (pointer >>> 32));
            return result;
        }
        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
                return true;
            if ( obj == null )
                return false;
            if ( getClass() != obj.getClass() )
                return false;
            GSP other = (GSP) obj;
            if ( generation != other.generation )
                return false;
            if ( pointer != other.pointer )
                return false;
            return true;
        }
    }

    static final int CHECKSUM_SIZE = 2;
    static final int SIZE =
            4 +             // generation (unsigned int)
            6 +             // pointer (6B long)
            CHECKSUM_SIZE;  // checksum for generation & pointer

    /**
     * Writes GSP at the given {@code offset}, the two fields (generation, pointer) + a checksum will be written.
     *
     * @param cursor {@link PageCursor} to write into.
     * @param gsp data to write.
     */
    public static void write( PageCursor cursor, GSP gsp )
    {
        assert (gsp.generation & ~0xFFFFFFFF) == 0;
        cursor.putInt( (int) gsp.generation );
        put6BLong( cursor, gsp.pointer );
        cursor.putShort( checksumOf( gsp ) );
    }

    /**
     * Reads GSP from the given {@code offset}, the two fields (generation, pointer) + a checksum will be read.
     * Generation and pointer will be matched against the read checksum.
     *
     * @param cursor {@link PageCursor} to read from.
     * @param gsp data structure to read into.
     * @return {@code true} if read checksum matches the read data, otherwise {@code false}. In any case
     * the fields in the given {@code gsp} may be changed as part of this call.
     */
    public static boolean read( PageCursor cursor, GSP gsp )
    {
        gsp.generation = cursor.getInt() & 0xFFFFFFFF;
        gsp.pointer = get6BLong( cursor );
        int checksum = cursor.getShort();
        return checksum == checksumOf( gsp );
    }

    private static long get6BLong( PageCursor cursor )
    {
        long lsb = cursor.getInt() & 0xFFFFFFFFL;
        long msb = cursor.getShort() & 0xFFFF;
        return lsb | (msb << Integer.SIZE);
    }

    private static void put6BLong( PageCursor cursor, long value )
    {
        int lsb = (int) value;
        short msb = (short) (value >>> Integer.SIZE);
        cursor.putInt( lsb );
        cursor.putShort( msb );
    }

    /**
     * Calculates a 2-byte checksum from GSP data.
     *
     * @param gsp data to calculate checksum for.
     * @return a {@code short} which is the checksum of the data in {@code gsp}.
     */
    public static short checksumOf( GSP gsp )
    {
        short result = 0;
        result ^= ((short) gsp.generation) & 0xFFFF;
        result ^= ((short) (gsp.generation >>> Short.SIZE)) & 0xFFFF;
        result ^= ((short) gsp.pointer) & 0xFFFF;
        result ^= ((short) (gsp.pointer >>> Short.SIZE)) & 0xFFFF;
        result ^= ((short) (gsp.pointer >>> Integer.SIZE)) & 0xFFFF;
        return result;
    }
}
