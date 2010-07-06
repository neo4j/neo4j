package org.neo4j.kernel.impl.ha;

import java.io.Serializable;

public final class IdAllocation implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final long[] ids;

    public IdAllocation( long[] ids )
    {
        this.ids = ids;
    }
    
    public long[] getIds()
    {
        return this.ids;
    }
}
