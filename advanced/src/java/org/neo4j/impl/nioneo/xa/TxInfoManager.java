/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.xa;

import org.neo4j.impl.transaction.xaframework.XaLogicalLog;

/**
 * Holds information about the current transaction such as recovery mode
 * and transaction identifier.
 */
public class TxInfoManager
{
	private static final TxInfoManager txManager = new TxInfoManager();
	
	private boolean recoveryMode = false;
	
	public static TxInfoManager getManager()
	{
		return txManager;
	}
	
	private XaLogicalLog log = null;
	
	public void setRealLog( XaLogicalLog log )
	{
		this.log = log;
	}
	
	void registerMode( boolean mode )
	{
		this.recoveryMode = mode;
	}
	
	void unregisterMode()
	{
		recoveryMode = false;
	}
	
	/**
	 * Returns <CODE>true</CODE> if the current transaction is in 
	 * recovery mode. If current thread doesn't have a transaction 
	 * <CODE>false</CODE> is returned.
	 * 
	 * @return True if current transaction is in recovery mode
	 */
	public boolean isInRecoveryMode()
	{
		return recoveryMode;
	}
	
	/**
	 * Returns the current transaction identifier. If current thread doesn't
	 * have a transaction <CODE>-1</CODE> is returned.
	 * 
	 * @return The current transaction identifier
	 */
	public int getCurrentTxIdentifier()
	{
		if ( log == null )
		{
			return -1;
		}
		return log.getCurrentTxIdentifier();
	}
}
