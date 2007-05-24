/*
 * Copyright 2002 Windh AB [www.windh.com]. All rights reserved.
 */
package unit.neo.transaction;

class MethodCall
{
	private String methodName = null;
	private Object args[] = null;
	private String signatures[] = null;
	
	MethodCall( String methodName, Object args[], String signatures[] )
	{
		if ( args.length != signatures.length )
		{
			throw new IllegalArgumentException( 
				"Args length not equal to signatures length." );
		}
		this.methodName = methodName;
		this.args = args;
		this.signatures = signatures;
	}
	
	String getMethodName()
	{
		return methodName;
	}
	
	Object[] getArgs()
	{
		return args;
	}
	
	String[] getSignatures()
	{
		return signatures;
	}
}