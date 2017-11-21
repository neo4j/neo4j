/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class CopiedStoreRecoveryTest
{
    @Test
    public void shouldThrowIfAlreadyShutdown() throws IOException
    {
        // Given
        CopiedStoreRecovery copiedStoreRecovery =
                new CopiedStoreRecovery( Config.defaults(), Iterables.empty(), mock( PageCache.class ) );
        copiedStoreRecovery.shutdown();

        try
        {
            // when
            copiedStoreRecovery.recoverCopiedStore( new File( "nowhere" ) );
            fail( "should have thrown" );
        }
        catch ( StoreCopyFailedException ex )
        {
            // then
            assertEquals( "Abort store-copied store recovery due to database shutdown", ex.getMessage() );
        }
    }
}
