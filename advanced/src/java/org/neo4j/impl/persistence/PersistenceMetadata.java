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
 *
 * An interface that provides meta data required by the persistence layer
 * about a persistent entity. The persistence layer uses this meta data to
 * for example select the data source that is used to persist the entity that
 * implements this interface.
 * <P>
 * Currently, this interface only contains an operation to obtain a reference
 * to the persistent entity.
 */
public interface PersistenceMetadata
{
	/**
	 * Returns the entity that wishes to participate in a persistence operation.
	 * @return the soon-to-be persistent entity
	 */
	public Object getEntity();
}
