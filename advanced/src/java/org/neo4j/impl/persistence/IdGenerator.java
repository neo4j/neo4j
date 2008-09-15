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
package org.neo4j.impl.persistence;

/**
 * The IdGenerator is responsible for generating unique ids for entities in the
 * Neo. The IdGenerator is configured via the {@link IdGeneratorModule}.
 * <P>
 * The IdGenerator must be loaded after its designated
 * {@link IdGeneratorModule#setPersistenceSource persistence source} during
 * startup.
 * <P>
 */
public class IdGenerator
{
    IdGenerator()
    {
    }

    // the persistence source used to store the HIGH keys
    private PersistenceSource persistenceSource = null;

    /**
     * Returns the next unique ID for the entity type represented by 
     * <CODE>clazz</CODE>.
     * @return the next ID for <CODE>clazz</CODE>'s entity type
     */
    public int nextId( Class<?> clazz )
    {
        return getPersistenceSource().nextId( clazz );
    }

    public int getHighestPossibleIdInUse( Class<?> clazz )
    {
        return getPersistenceSource().getHighestPossibleIdInUse( clazz );
    }

    public int getNumberOfIdsInUse( Class<?> clazz )
    {
        return getPersistenceSource().getNumberOfIdsInUse( clazz );
    }

    /**
     * Configures the IdGenerator. <B>WARNING</B>: This method should only be
     * invoked once from {@link IdGeneratorModule#start}.
     * @param source
     *            the persistence source used for id generation
     */
    void configure( PersistenceSource source )
    {
        // Save connectivity
        this.persistenceSource = source;
    }

    // Accesor for persistence source
    private PersistenceSource getPersistenceSource()
    {
        return this.persistenceSource;
    }
}