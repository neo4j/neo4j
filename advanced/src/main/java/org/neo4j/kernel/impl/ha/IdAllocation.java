package org.neo4j.kernel.impl.ha;

import org.neo4j.kernel.impl.nioneo.store.IdRange;

public final class IdAllocation
{
    private final IdRange idRange;
    private final long highestIdInUse;
    private final long defragCount;

    public IdAllocation( IdRange idRange, long highestIdInUse, long defragCount )
    {
        this.idRange = idRange;
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

    public IdRange getIdRange()
    {
        return this.idRange;
    }
}
