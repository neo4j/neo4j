package org.neo4j.impl.transaction.xaframework;

import java.util.Map;

/**
 * <CODE>XaDataSource</CODE> is as a factory for creating 
 * {@link XaConnection XaConnections}. 
 * <p>
 * If you're writing a data source towards a XA compatible resource the 
 * implementation should be fairly straight forward. This will basically be 
 * a factory for your {@link XaConnection XaConnections} that in turn will 
 * wrap the XA compatible <CODE>XAResource</CODE>.
 * <p>
 * If you're writing a data source towards a non XA compatible resource use the
 * {@link XaContainer} and extend the {@link XaConnectionHelpImpl} as your 
 * {@link XaConnection} implementation. Here is an example: <p>
 * <pre>
 * <CODE>
 * public class MyDataSource implements XaDataSource {
 *     MyDataSource( params ) {
 *         // ... initalization stuff
 *         container = XaContainer.create( myLogicalLogFile, myCommandFactory, 
 *             myTransactionFactory );
 *         // ... more initialization stuff
 *         container.openLogicalLog();
 *     }
 *
 *     public XaConnection getXaConnection() {
 *         return new MyXaConnection( params );
 *     }
 *
 *     public void close() {
 *         // ... cleanup
 *         container.close();
 *     }
 * }
 * </CODE>
 * </pre>
 */
public abstract class XaDataSource
{
	/**
	 * Constructor used by the Neo to create datasources.  
	 *
	 * @param params A map containing configuration parameters
	 */
	public XaDataSource( Map params ) 
		throws InstantiationException
	{
	}
	
	/**
	 * Creates a XA connection to the resource this data source represents.
	 *
	 * @return A connection to an XA resource
	 */
	public abstract XaConnection getXaConnection();
	
	/**
	 * Closes this data source. Calling <CODE>getXaConnection</CODE> after 
	 * this method has been invoked is illegal.
	 */
	public abstract void close();
}
