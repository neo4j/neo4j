/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.IndexState;

/**
 * Contract for implementing an index in Neo4j.
 *
 * This is a sensitive thing to implement, because it manages data that is controlled by
 * Neo4js logical log. As such, the implementation needs to behave under some rather strict rules.
 *
 * <h3>Populating the index</h3>
 *
 * When an index rule is added, the {@link IndexingService} is notified. It will, in turn, ask
 * your {@link SchemaIndexProvider} for a {@link #getPopulator(long) batch index writer}.
 *
 * A background index job is triggered, and all existing data that applies to the new rule, as well as new data
 * from the "outside", will be inserted using the writer. You are guaranteed that usage of this writer,
 * during population, will be single threaded.
 *
 * These are the rules you must adhere to here:
 *
 * <ul>
 * <li>You CANNOT say that the state of the index is {@link org.neo4j.kernel.api.IndexState#ONLINE}</li>
 * <li>You MUST store all updates given to you</li>
 * <li>You MAY persistently store the updates</li>
 * </ul>
 *
 *
 * <h3>The Flip</h3>
 *
 * Once population is done, the index needs to be "flipped" to an online mode of operation.
 *
 * The index will be notified, through the {@link org.neo4j.kernel.impl.api.index.IndexPopulator#populationCompleted()}
 * method, that population is done, and that the index should turn it's state to {@link org.neo4j.kernel.api
 * .IndexState#ONLINE}.
 *
 * If the index is persisted to disk, this is a <i>vital</i> part of the index lifecycle.
 * For a persisted index, the index MUST NOT store the state as online unless it first guarantees that the entire index
 * is flushed to disk. Failure to do so could produce a situation where, after a crash,
 * an index is believed to be online
 * when it in fact was not yet fully populated. This would break the database recovery process.
 *
 * If you are implementing this interface, you can choose to not store index state. In that case,
 * you should report index state as {@link org.neo4j.kernel.api.IndexState#NON_EXISTENT} upon startup.
 * This will cause the database to re-create the index from scratch again.
 *
 * These are the rules you must adhere to here:
 *
 * <ul>
 * <li>You MUST have flushed the index to durable storage if you are to persist index state as {@link org.neo4j
 * .kernel.api.IndexState#ONLINE}</li>
 * <li>You MAY decide not to store index state</li>
 * <li>If you don't store index state, you MUST default to {@link org.neo4j.kernel.api.IndexState#NON_EXISTENT}</li>
 * </ul>
 *
 * <h3>Online operation</h3>
 *
 * Once the index is online, the database will move to using the {@link #getWriter(long) index writer} to write to the
 * index.
 */
public abstract class SchemaIndexProvider extends Service
{
    /**
     * Create a new instance of a service implementation identified with the
     * specified key(s).
     *
     * @param key the main key for identifying this service implementation
     */
    protected SchemaIndexProvider( String key )
    {
        super( key );
    }

    public abstract IndexPopulator getPopulator( long indexId );

    public abstract IndexWriter getWriter( long indexId );

    // Design idea: we add methods here like:
    //    getReader( IndexDefinition index )

    public abstract IndexState getState( long indexId );

    /**
     * For persistent indexes make sure all data in the indexes are forced to disk.
     * This method isn't allowed to return until all I/O has completed.
     */
    public abstract void flushAll();
}
