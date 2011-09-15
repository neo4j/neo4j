package org.neo4j.kernel.impl.storemigration;

import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;

import java.nio.ByteBuffer;

public class LegacyStore
{
    static final String FROM_VERSION = "NodeStore v0.9.9";

    public static long getUnsignedInt(ByteBuffer buf)
    {
        return buf.getInt()&0xFFFFFFFFL;
    }

    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base|modifier;
    }
}
