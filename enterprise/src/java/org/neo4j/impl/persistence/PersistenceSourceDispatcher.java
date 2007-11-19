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

// JTA imports
import java.util.logging.Logger;

/**
 * The persistence layer component responsible for demarcating the
 * persistent entity space and dispatching persistent state to
 * designated persistence sources.
 * <P>
 * It's the PersistenceSourceDispatcher's job to ensure that, for
 * example, node <CODE>42</CODE> is persisted in persistence source
 * <CODE>A</CODE> while node <CODE>43</CODE> is persisted in persistence
 * source <CODE>B</CODE>.
 * <P>
 * The present implementation, however, only supports one persistence source.
 * <P>
 * Normally, the only component that interacts with the
 * PersistenceSourceDispatcher is the {@link ResourceBroker}.
 */
class PersistenceSourceDispatcher
{
	private static Logger log = Logger.getLogger(
		PersistenceSourceDispatcher.class.getName() );

	private PersistenceSource ourOnlyPersistenceSource = null;
	
	
	private static final PersistenceSourceDispatcher instance =
		new PersistenceSourceDispatcher();
	
	private PersistenceSourceDispatcher() { }
	
	/**
	 * Singleton accessor.
	 * @return the singleton persistence source dispatcher
	 */
	static PersistenceSourceDispatcher getDispatcher()
	{
		return instance;
	}
	
	/**
	 * Returns the persistence source that should be used to persist
	 * <CODE>obj</CODE>.
	 * @param obj the soon-to-be-persisted entity
	 * @return the persistence source for <CODE>obj</CODE>
	 */
	PersistenceSource getPersistenceSource() // Object obj )
	{
		return this.ourOnlyPersistenceSource;
	}
	
	/**
	 * Adds a persistence source to the dispatcher's internal
	 * list of available persistence sources. If the dispatcher
	 * already knows about <CODE>source</CODE>, this method fails
	 * silently with a message in the logs.
	 * @param source the new persistence source
	 */
	void persistenceSourceAdded( PersistenceSource source )
	{
		this.ourOnlyPersistenceSource = source;
	}
	
	/**
	 * Removes a persistence source from the dispatcher's internal
	 * list of available persistence sources. If the dispatcher
	 * is unaware of <CODE>source</CODE>, this method fails
	 * silently with a message in the logs.
	 * @param source the persistence source that will be removed
	 */
	void persistenceSourceRemoved( PersistenceSource source )
	{
		if ( this.ourOnlyPersistenceSource == source )
		{
			this.ourOnlyPersistenceSource = null;
		}
		else
		{
			log.severe(	source + " was just removed, but as far as we're " +
						"concerned, it has never been added." );
		}
	}
}
