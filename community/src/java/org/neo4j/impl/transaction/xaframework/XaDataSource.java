/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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

import java.util.Map;

/**
 * <CODE>XaDataSource</CODE> is as a factory for creating
 * {@link XaConnection XaConnections}.
 * <p>
 * If you're writing a data source towards a XA compatible resource the
 * implementation should be fairly straight forward. This will basically be a
 * factory for your {@link XaConnection XaConnections} that in turn will wrap
 * the XA compatible <CODE>XAResource</CODE>.
 * <p>
 * If you're writing a data source towards a non XA compatible resource use the
 * {@link XaContainer} and extend the {@link XaConnectionHelpImpl} as your
 * {@link XaConnection} implementation. Here is an example:
 * <p>
 * 
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
 *     public XaConnection getXaCo nnection() {
 *         return new MyXaConnection( params );
 *     }
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
     * @param params
     *            A map containing configuration parameters
     */
    public XaDataSource( Map<?,?> params ) throws InstantiationException
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

    public abstract void setBranchId( byte branchId[] );

    public abstract byte[] getBranchId();

}