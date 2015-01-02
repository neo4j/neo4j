/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.persistence;

import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

/**
 * A data resource that Neo4j kernel uses to persist entities.
 * <P>
 * All persistence sources for the kernel should implement this interface.
 */
public interface PersistenceSource
{
    /**
     * Creates a resource connection to this persistence source.
     * @param connection the {@link XaConnection} to use.
     * @return a newly opened {@link NeoStoreTransaction} to this
     *         PersistenceSource
     */
    NeoStoreTransaction createTransaction( XaConnection connection );

    /**
     * If the persistence source is responsible for id generation it must
     * implement this method.
     * 
     * @param clazz
     *            the data structure to get next free unique id for
     * @return the next free unique id for <CODE>clazz</CODE>
     */
    long nextId( Class<?> clazz );

    long getHighestPossibleIdInUse( Class<?> clazz );

    long getNumberOfIdsInUse( Class<?> clazz );
    
    XaDataSource getXaDataSource();
}