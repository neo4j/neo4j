/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.junit.Test;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThreadToStatementContextBridgeTest
{
    @Test
    public void shouldThrowNotInTransactionExceptionWhenNotInTransaction() throws Exception
    {
        // Given
        PersistenceManager persistenceManager = mock( PersistenceManager.class );
        when( persistenceManager.currentKernelTransactionForReading() ).thenReturn( null );
        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge( persistenceManager );

        // When
        try
        {
            bridge.instance();
            fail( "Should throw" );
        }
        catch ( NotInTransactionException e )
        {   // Good
        }
    }
}
