/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

public class TestIndexProviderStore
{
    @Test
    public void lastCommitedTxGetsStoredBetweenSessions() throws Exception
    {
        File file = new File( "target/test-data/index-provider-store" );
        file.mkdirs();
        file.delete();
        IndexProviderStore store = new IndexProviderStore( file );
        store.setVersion( 5 );
        store.setLastCommittedTx( 12 );
        store.close();
        store = new IndexProviderStore( file );
        assertEquals( 5, store.getVersion() );
        assertEquals( 12, store.getLastCommittedTx() );
        store.close();
    }
}
