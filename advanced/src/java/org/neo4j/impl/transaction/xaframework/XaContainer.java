package org.neo4j.impl.transaction.xaframework;

import java.io.IOException;


/**
 * This is a wrapper class containing the logical log, command factory, 
 * transaction factory and resource manager. Using the static 
 * <CODE>create</CODE> method, a {@link XaResourceManager} will be created 
 * and the {@link XaLogicalLog} will be instantiated (not opened).
 * <p>
 * The purpose of this class is to make it easier to setup all that is needed
 * when using the <CODE>xaframework</CODE>. Classes needs to be instantiated 
 * in a certain order and references passed between the. 
 * <CODE>XaContainer</CODE> will do that work so you only have to supply 
 * the <CODE>logical log file</CODE>, your {@link XaCommandFactory} and 
 * {@link XaTransactionFactory} implementations.
 */
public class XaContainer
{
	private XaCommandFactory cf = null;
	private XaLogicalLog log = null;
	private XaResourceManager rm = null;
	private XaTransactionFactory tf = null;
	
	/**
	 * Creates a XaContainer.
	 *
	 * @param logicalLog The logical log file name
	 * @param cf The command factory implementation
	 * @param tf The transaction factory implementation
	 */
	public static XaContainer create( String logicalLog, XaCommandFactory cf, 
		XaTransactionFactory tf ) // throws IOException
	{
		if ( logicalLog == null || cf == null || tf == null )
		{
			throw new IllegalArgumentException( "Null parameter, " +
				"LogicalLog[" + logicalLog + "] CommandFactory[" + cf +
				"TransactionFactory[" + tf + "]" );
		}
		return new XaContainer( logicalLog, cf, tf );
	}
	
	private XaContainer( String logicalLog, XaCommandFactory cf, 
		XaTransactionFactory tf ) // throws IOException
	{
		this.cf = cf;
		this.tf = tf;
		rm = new XaResourceManager( tf );
		log = new XaLogicalLog( logicalLog, rm, cf, tf );
		rm.setLogicalLog( log );
		tf.setLogicalLog( log );
	}
	
	/**
	 * Opens the logical log. If the log doesn't exist a new log will be 
	 * created. If the log exists it will be scaned and non completed 
	 * transactions will be recovered.
	 * <p>
	 * This method is only valid to invoke once after the container has been
	 * created. Invoking this method again could cause the logical log to 
	 * be corrupted.
	 */
	public void openLogicalLog() throws IOException
	{
		log.open();
	}
	
	/** 
	 * Closes the logical log and nulls out all instances. 
	 */
	public void close()
	{
		try
		{
			if ( log != null )
			{
				log.force();
				log.close();
			}
		}
		catch ( IOException e )
		{
			System.out.println( "Unable to close logical log" );
			e.printStackTrace();
		}
		log = null;
		rm = null;
		cf = null;
		tf = null;
	}
	
	public XaCommandFactory getCommandFactory()
	{
		return cf;
	}
	
	public XaLogicalLog getLogicalLog()
	{
		return log;
	}
	
	public XaResourceManager getResourceManager()
	{
		return rm;
	}
	
	public XaTransactionFactory getTransactionFactory()
	{
		return tf;
	}
}
