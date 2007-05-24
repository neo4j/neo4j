package org.neo4j.impl.transaction;

import javax.transaction.xa.Xid;

class XidImpl implements Xid
{
	private static final int FORMAT_ID = 0x4E454E31; // NEO format identidier
	
	// hardcoded for now, we don't have distribution yet but
	// this should change later so one at least can run more than one
	// kernel on same machine (next step is distributed kernels)
	private static final byte INSTANCE_ID[] = new byte[]  
		{ 'N', 'E', 'O', 'K', 'E', 'R', 'N', 'L' };
		
	private static long nextSequenceId = 0;

	// next sequence number for global tx id
	private synchronized static long getNextSequenceId()
	{
		return nextSequenceId++;
	}
	
	// INSTANCE_ID + millitime(long) + seqnumber(long) 
	private byte globalId[] = null; 
	// branchId asumes Xid.MAXBQUALSIZE >= 4
	private byte branchId[] = null;
	
	// resourceId.length = 4, unique for each XAResource
	static byte[] getNewGlobalId()
	{
		// create new global id ( [INSTANCE_ID][time][sequence] )
		byte globalId[] = new byte[ INSTANCE_ID.length + 16 ];
		System.arraycopy( INSTANCE_ID, 0, globalId, 0, INSTANCE_ID.length );
		long time = System.currentTimeMillis();
		long sequence = getNextSequenceId();
		for ( int i = 0; i < 8; i++ )
		{
			globalId[ INSTANCE_ID.length + i ] = 
				( byte ) ( ( time >> ( ( 7 - i ) * 8 ) ) & 0xFF );
		}
		for ( int i = 0; i < 8; i++ )
		{
			globalId[ INSTANCE_ID.length + 8 + i ] = 
				( byte ) ( ( sequence >> ( ( 7 - i ) * 8 ) ) & 0xFF );
		}
		return globalId;
	}
	
	static boolean isThisTm( byte globalId[] )
	{
		if ( globalId.length < INSTANCE_ID.length )
		{
			return false;
		}
		for ( int i = 0; i < INSTANCE_ID.length; i++ )
		{
			if ( globalId[i] != INSTANCE_ID[i] )
			{
				return false;
			}
		}
		return true;
	}
	
	// create xid for transaction with more than one XAResource enlisted
	XidImpl( byte globalId[], byte resourceId[] )
	{
		if ( globalId.length > Xid.MAXGTRIDSIZE )
		{
			throw new IllegalArgumentException( "GlobalId length to long, " + 
				globalId.length );
		}
		if ( resourceId.length > Xid.MAXBQUALSIZE )
		{
			throw new IllegalArgumentException( 
				"BranchId (resource id) to long, " + resourceId.length );
		}
		this.globalId = globalId;
		this.branchId = resourceId;
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
		for ( int i = 0; i < INSTANCE_ID.length; i++ )
		{
			buf.append( ( char ) globalId[i] );
		}
		long time = 0;
		for ( int i = 0; i < 8; i++ )
		{
			time += ( long ) ( globalId[ INSTANCE_ID.length + i ] & 0xFF ) << 
				( ( 7 - i ) * 8 );
		}
		buf.append( '|' );
		buf.append( new java.util.Date( time ).toString() );
		long sequence = 0;
		for ( int i = 0; i < 8; i++ )
		{
			sequence += ( long ) 
				( globalId[ INSTANCE_ID.length + 8 + i ] & 0xFF ) << 
					( ( 7 - i ) * 8 );
		}
		buf.append( '|' );
		buf.append( sequence );
		buf.append( "], BranchId[" );
		for ( int i = 0; i < branchId.length; i++ )
		{
			buf.append( ( char ) branchId[i] );
		}
		buf.append( "]" );
		return buf.toString();
	}
}
