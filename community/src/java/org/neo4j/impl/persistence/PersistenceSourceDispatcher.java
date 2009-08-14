/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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

import java.util.logging.Logger;

class PersistenceSourceDispatcher
{
    private static Logger log = Logger
        .getLogger( PersistenceSourceDispatcher.class.getName() );

    private PersistenceSource ourOnlyPersistenceSource = null;

    PersistenceSourceDispatcher()
    {

    }

    /**
     * Returns the persistence source that should be used to persist <CODE>obj</CODE>.
     * @param obj
     *            the soon-to-be-persisted entity
     * @return the persistence source for <CODE>obj</CODE>
     */
    PersistenceSource getPersistenceSource()
    {
        return this.ourOnlyPersistenceSource;
    }

    /**
     * Adds a persistence source to the dispatcher's internal list of available
     * persistence sources. If the dispatcher already knows about <CODE>source</CODE>,
     * this method fails silently with a message in the logs.
     * @param source
     *            the new persistence source
     */
    void persistenceSourceAdded( PersistenceSource source )
    {
        this.ourOnlyPersistenceSource = source;
    }

    /**
     * Removes a persistence source from the dispatcher's internal list of
     * available persistence sources. If the dispatcher is unaware of <CODE>source</CODE>,
     * this method fails silently with a message in the logs.
     * @param source
     *            the persistence source that will be removed
     */
    void persistenceSourceRemoved( PersistenceSource source )
    {
        if ( this.ourOnlyPersistenceSource == source )
        {
            this.ourOnlyPersistenceSource = null;
        }
        else
        {
            log.severe( source + " was just removed, but as far as we're "
                + "concerned, it has never been added." );
        }
    }
}