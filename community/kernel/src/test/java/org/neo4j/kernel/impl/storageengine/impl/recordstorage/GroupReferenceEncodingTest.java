package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupReferenceEncodingTest
{
    // This value the largest possible high limit id +1 (see HighLimitV3_1_0)
    private static long MAX_ID_LIMIT = 1L << 50;

    @Test
    public void encodeRelationship()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( GroupReferenceEncoding.isRelationship( reference ) );
            assertTrue( GroupReferenceEncoding.isRelationship( GroupReferenceEncoding.encodeRelationship( reference ) ) );
            assertTrue( "encoded reference is negative", GroupReferenceEncoding.encodeRelationship( reference ) < 0 );
        }
    }
}
