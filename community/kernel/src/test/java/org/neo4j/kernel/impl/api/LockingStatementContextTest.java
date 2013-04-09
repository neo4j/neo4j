/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.transaction.LockType;

public class LockingStatementContextTest
{
    @Test
    public void shouldGrabWriteLocksBeforeDeleting() throws Exception
    {
        // GIVEN
        StatementContext inner = mock( StatementContext.class );
        LockHolder lockHolder = mock( LockHolder.class );
        NodeProxy.NodeLookup lookup = mock( NodeProxy.NodeLookup.class );
        NodeImpl node = mock( NodeImpl.class );
        int nodeId = 0;

        when( lookup.lookup( anyLong(), any( LockType.class ) ) ).thenReturn( node );

        LockingStatementContext statementContext = new LockingStatementContext( inner, lockHolder );

        // WHEN
        statementContext.deleteNode( nodeId );

        //THEN
//        verify( inner ).deleteNode( null, nodeId );
        verify( lockHolder ).acquireNodeWriteLock( nodeId );
    }

}
