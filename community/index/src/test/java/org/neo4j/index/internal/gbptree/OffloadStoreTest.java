package org.neo4j.index.internal.gbptree;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class OffloadStoreTest
{
    private static final int PAGE_SIZE = 256;

    private static final PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( 256 );
    private static final SimpleByteArrayLayout layout = new SimpleByteArrayLayout( false );

    @Test
    void mustReadKey() throws IOException
    {
        OffloadStore<RawBytes,RawBytes> offloadStore = new OffloadStore<>( layout );

        RawBytes key = layout.newKey();
        key.bytes = new byte[200];
        long offloadId = offloadStore.writeKey( key );

        RawBytes into = layout.newKey();
        offloadStore.readKey( offloadId, into );

        assertEquals( 0, layout.compare( key, into ) );
    }
}
