package org.neo4j.kernel.impl.store.format;


import org.neo4j.kernel.impl.store.format.highlimit.EnterpriseHighLimit;

public class EnterpriseHighLimitRecordFormatTest extends RecordFormatTest
{
    protected static final RecordGenerators _58_BIT_LIMITS = new LimitedRecordGenerators( random, 58, 58, 58, 16, NULL );
    protected static final RecordGenerators _50_BIT_LIMITS = new LimitedRecordGenerators( random, 50, 50, 50, 16, NULL );

    public EnterpriseHighLimitRecordFormatTest()
    {
        super( EnterpriseHighLimit.RECORD_FORMATS, _50_BIT_LIMITS );
    }
}
