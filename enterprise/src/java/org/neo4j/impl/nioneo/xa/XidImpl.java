package org.neo4j.impl.nioneo.xa;

import javax.transaction.xa.Xid;

/**
 * Xid implementation, made public only for testing purpouse.  
 */
public class XidImpl implements Xid
{
	private static final int FORMAT_ID = 0x4E454E31; // NEO format identidier
	
	private byte globalId[] = null; 
	private byte branchId[] = null;

	public XidImpl( byte globalId[], byte branchId[] )
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
		return FORMAT_ID;
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
