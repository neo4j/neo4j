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
package org.neo4j.kernel;

import org.neo4j.graphdb.GraphDatabaseService;

public abstract class AbstractGraphDatabase implements GraphDatabaseService, GraphDatabaseAPI
{
    /**
     * @deprecated This method is only for internal use.
     * Version 1.9 of Neo4j will be the last version to contain this method.
     */
    @Deprecated
    public abstract boolean transactionRunning();

    /**
     * Get a single management bean. Delegates to {@link #getSingleManagementBean(Class)}.
     *
     * @deprecated since Neo4j may now have multiple beans implementing the same bean interface, this method has been
     *             deprecated in favor of {@link #getSingleManagementBean(Class)} and {@link #getManagementBeans(Class)}
     *             . Version 1.5 of Neo4j will be the last version to contain this method.
     */
    @Deprecated
    public abstract <T> T getManagementBean( Class<T> type );
}
