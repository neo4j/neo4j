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
package org.neo4j.kernel.impl.event;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

public class VerifyingTransactionEventHandler implements
        TransactionEventHandler<Object>
{
    private final ExpectedTransactionData expectedData;
    private boolean hasBeenCalled;

    public VerifyingTransactionEventHandler( ExpectedTransactionData expectedData )
    {
        this.expectedData = expectedData;
    }
    
    public void afterCommit( TransactionData data, Object state )
    {
    }

    public void afterRollback( TransactionData data, Object state )
    {
    }

    public Object beforeCommit( TransactionData data ) throws Exception
    {
        this.expectedData.compareTo( data );
        this.hasBeenCalled = true;
        return null;
    }
    
    boolean hasBeenCalled()
    {
        return this.hasBeenCalled;
    }
}
