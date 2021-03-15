package org.neo4j.kernel.database;

import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.IOController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertSame;

@DbmsExtension
class CommunityIOControllerIO
{
    @Inject
    private IOController ioController;

    @Test
    void useCommunityIOController()
    {
        assertSame( ioController, IOController.DISABLED );
    }
}
