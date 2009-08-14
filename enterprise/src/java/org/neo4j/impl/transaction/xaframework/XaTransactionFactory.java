/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.transaction.xaframework;


/**
 * Factory for creating {@link XaTransaction XaTransactions} used during
 * recovery.
 */
public abstract class XaTransactionFactory
{
    private XaLogicalLog log;

    /**
     * Create a {@link XaTransaction} with <CODE>identifier</CODE> as internal
     * transaction id.
     * 
     * @param identifier
     *            The identifier of the transaction
     * @return A new xa transaction
     */
    public abstract XaTransaction create( int identifier );

    public abstract void flushAll();
    
    void setLogicalLog( XaLogicalLog log )
    {
        this.log = log;
    }

    protected XaLogicalLog getLogicalLog()
    {
        return log;
    }

    /**
     * This method will be called when all recovered transactions have been
     * resolved to a safe state (rolledback or committed). This implementation
     * does nothing so override if you need to do something when recovery is
     * complete.
     */
    public void recoveryComplete()
    {
    }

    public abstract long getCurrentVersion();
    
    public abstract long getAndSetNewVersion();
    
    
} 