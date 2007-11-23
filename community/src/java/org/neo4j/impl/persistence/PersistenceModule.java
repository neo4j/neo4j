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
package org.neo4j.impl.persistence;

import javax.transaction.TransactionManager;
import org.neo4j.impl.event.EventListenerNotRegisteredException;
import org.neo4j.impl.event.EventManager;

/**
 *
 * This class represents the persistence module. It receives lifecycle
 * events from the module framework.

 */
public class PersistenceModule
{
	// -- Constants
	private static final String	MODULE_NAME	= "PersistenceModule";
	
	private final PersistenceManager persistenceManager;
	private final PersistenceLayerMonitor persistenceMonitor;
	
	private PersistenceSource source;
	
	public PersistenceModule( EventManager eventManager, 
		TransactionManager transactionManager )
	{
		persistenceManager = new PersistenceManager( transactionManager );
		ResourceBroker broker = persistenceManager.getResourceBroker();
		persistenceMonitor = new PersistenceLayerMonitor( eventManager, 
			broker );
	}
	
	public synchronized void init()
	{
		// Do nothing
	}
	
	public synchronized void start( PersistenceSource persistenceSource )
	{
		this.source = persistenceSource;
		persistenceMonitor.registerPersistenceSource( source );
		try
		{
			persistenceMonitor.registerEvents();
		}
		catch ( EventListenerNotRegisteredException elnre )
		{
			// TODO:	This is a fatal thing. We really should shut down the
			//			Neo engine if something messes up during the startup of
			//			the persistence module. How do we tell the module
			//			framework to request emergency shutdown?
			throw new RuntimeException( "The persistence layer monitor was " +
										  "unable to register on one or more " +
										  "event types. This can seriously " +
										  "impact persistence operations.",
										  elnre );
		}
	}
	
	public synchronized void reload()
	{
		this.stop();
		persistenceMonitor.removePersistenceSource( source );
	}
	
	public synchronized void stop()
	{
		persistenceMonitor.unregisterEvents();
	}
	
	public synchronized void destroy()
	{
		// Do nothing
	}
	
	public String getModuleName()
	{
		return MODULE_NAME;
	}
	
	public PersistenceManager getPersistenceManager()
	{
		return persistenceManager;
	}
}