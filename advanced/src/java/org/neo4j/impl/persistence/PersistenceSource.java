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

/**
 * A data resource that Neo uses to persist entities.
 * Examples of a <CODE>PersistenceSource</CODE> include a database server
 * or an LDAP server.
 * <P>
 * All persistence sources for Neo should implement this interface.
 * On bootstrap, the persistence source generates a
 * {@link org.neo4j.impl.event.Event#DATA_SOURCE_ADDED DATA_SOURCE_ADDED}
 * event, which the persistence framework receives and uses to include the
 * persistence source in all future persistence operations.
 * <P>
 */
public interface PersistenceSource
{
	/**
	 * Creates a resource connection to this persistence source.
	 * @return a newly opened {@link ResourceConnection} to this
	 * PersistenceSource
	 * @throws ConnectionCreationFailedException if unable to open a new
	 * ResourceConnection to this PersistenceSource
	 */
	public ResourceConnection createResourceConnection()
		throws ConnectionCreationFailedException;
		
	/**
	 * If the persistence source is responsible for id generation it must 
	 * implement this method. If the persistence source is unable to generate 
	 * id for any reason a {@link IdGenerationFailedException} should be thrown.
	 *
	 * @param clazz the data structure to get next free unique id for
	 * @return the next free unique id for <CODE>clazz</CODE>
	 */
	public int nextId( Class<?> clazz );
	
	public int getHighestPossibleIdInUse( Class<?> clazz );
	
	public int getNumberOfIdsInUse( Class<?> clazz );
}
