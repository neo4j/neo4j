package org.neo4j.kernel.impl.ha;

import java.io.Serializable;

public final class IdAllocation implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final long[] ids;
    private final long highestIdInUse;
    private final long defragCount;

    public IdAllocation( long[] ids, long highestIdInUse, long defragCount )
    {
        this.ids = ids;
        this.highestIdInUse = highestIdInUse;
        this.defragCount = defragCount;
    }
    
    public long getHighestIdInUse()
    {
        return highestIdInUse;
    }

    public long getDefragCount()
    {
        return defragCount;
    }

    public long[] getIds()
    {
        return this.ids;
    }
}
