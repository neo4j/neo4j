package org.neo4j.impl.transaction.xaframework;

import javax.transaction.xa.Xid;

class XidImpl implements Xid
{
	private int formatId = -1;
	private byte globalId[] = null; 
	private byte branchId[] = null;

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
		return ( byte[] ) globalId.clone();
	}

	public byte[] getBranchQualifier()
	{
		return ( byte[] ) branchId.clone();
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
