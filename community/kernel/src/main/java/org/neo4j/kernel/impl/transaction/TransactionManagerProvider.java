/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.StringLogger;


/**
 * Hook in the kernel module that providers of TransactionManagers must extend.
 * To implement an alternative TransactionManager as a service to be discovered
 * and loaded by the neo tx handling code at startup, you must extend this
 * class.
 *
 * @author Chris Gioran
 * @author Tobias Ivarsson
 *
 */
public abstract class TransactionManagerProvider extends Service
{
    public TransactionManagerProvider( String name )
    {
        super( name );
    }

    public abstract AbstractTransactionManager loadTransactionManager( String txLogDir,
    		XaDataSourceManager xaDataSourceManager,
    		KernelPanicEventGenerator kpe, 
    		TxHook rollbackHook, 
    		StringLogger msgLog, 
    		FileSystemAbstraction fileSystem,
    		TransactionStateFactory stateFactory );
}
