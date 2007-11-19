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
package org.neo4j.impl.transaction.xaframework;

import javax.transaction.xa.Xid;

class XidImpl implements Xid
{
	private final int formatId;
	private final byte globalId[]; 
	private final byte branchId[];

	public XidImpl( byte globalId[], byte branchId[], int formatId )
	{
		if ( globalId.length > Xid.MAXGTRIDSIZE )
		{
			throw new IllegalArgumentException( "GlobalId length to long, " + 
				globalId.length );
		}
		if ( branchId.length > Xid.MAXBQUALSIZE )
		{
			throw new IllegalArgumentException( 
				"BranchId (resource id) to long, " + branchId.length );
		}
		this.globalId = globalId;
		this.branchId = branchId;
		this.formatId = formatId;
	}
	
	public byte[] getGlobalTransactionId()
	{
		return globalId.clone();
	}

	public byte[] getBranchQualifier()
	{
		return branchId.clone();
	}
	
	public int getFormatId()
	{
		return formatId;
	}
	
	public boolean equals( Object o ) 
	{
		if ( !( o instanceof Xid ) )
		{
			return false;
		}
		byte otherGlobalId[] = ( ( Xid ) o ).getGlobalTransactionId();
		byte otherBranchId[] = ( ( Xid ) o ).getBranchQualifier();

		if ( globalId.length != otherGlobalId.length || 
			branchId.length != otherBranchId.length )
		{
			return false;
		}
		
		for ( int i = 0; i < globalId.length; i++ )
		{
			if ( globalId[i] != otherGlobalId[i] )
			{
				return false;
			}
		}
		for ( int i = 0; i < branchId.length; i++ )
		{
			if ( branchId[i] != otherBranchId[i] )
			{
				return false;
			}
		}
		return true;
	}
	
	private volatile int hashCode = 0;

	public int hashCode()
	{
		if ( hashCode == 0 )
		{
			int calcHash = 0;
			for ( int i = 0; i < 4 && i < globalId.length; i++ )
			{
				calcHash += globalId[ globalId.length - i - 1 ] << i * 8;
			}
			hashCode = 3217 * calcHash;
		}
		return hashCode;
	}
	
	public String toString()
	{
		StringBuffer buf = new StringBuffer( "GlobalId[" );
		for ( int i = 0; i < globalId.length; i++ )
		{
			buf.append( ( char ) globalId[i] );
		}
		buf.append( "], BranchId[" );
		for ( int i = 0; i < branchId.length; i++ )
		{
			buf.append( ( char ) branchId[i] );
		}
		buf.append( "]" );
		return buf.toString();
	}
}
