package org.neo4j.kernel.impl.store.format.lowlimit;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.store.format.RecordFormats;

@Service.Implementation(RecordFormats.Factory.class)
public class LowLimitFactory extends RecordFormats.Factory
{
    public LowLimitFactory()
    {
        super( "lowlimit" );
    }

    @Override
    public RecordFormats newInstance( )
    {
        return LowLimit.RECORD_FORMATS;
    }
}
