package org.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.store.format.RecordFormats;

@Service.Implementation(RecordFormats.Factory.class)
public class HighLimitFactory extends RecordFormats.Factory
{
    public HighLimitFactory()
    {
        super( "highlimit" );
    }

    @Override
    public RecordFormats newInstance()
    {
        return HighLimit.RECORD_FORMATS;
    }
}
