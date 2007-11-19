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
package org.neo4j.impl.core;

import java.util.logging.Logger;

import javax.transaction.Synchronization;

class TxCommitHook implements Synchronization
{
	private static Logger log = Logger.getLogger( 
		TxCommitHook.class.getName() );
	
	public void beforeCompletion()
	{
	}

	public void afterCompletion( int param )
	{
		try
		{
			this.releaseLocks( param );
		}
		catch ( Throwable t )
		{
			t.printStackTrace();
			log.severe( "Unable to release commands" );
		}
	}
	
	private void releaseLocks( int param )
	{
		LockReleaser.getManager().releaseLocks();
	}
}

