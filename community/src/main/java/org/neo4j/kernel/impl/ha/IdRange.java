package org.neo4j.kernel.impl.ha;

import java.io.Serializable;

public final class IdRange implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final long low;
    private final long high;

    public IdRange( long low, long high )
    {
        this.low = low;
        this.high = high;
    }

    public long getLower()
    {
        return low;
    }

    public long getUpper()
    {
        return high;
    }
}
