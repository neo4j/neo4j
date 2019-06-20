/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.counts;

import java.io.IOException;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Store and accessor of entity counts. Counts changes revolves around one or a combination of multiple tokens and are applied as deltas.
 * This makes it necessary to tie all changes to transaction ids so that this store can tell whether or not to re-apply any given
 * set of changes during recovery. Changes are applied by making calls to {@link CountsAccessor.Updater} from {@link #apply(long)}.
 */
public interface CountsStore extends CountsAccessor, AutoCloseable
{
    /**
     * @param txId id of the transaction that produces the changes that are being applied.
     * @return an updater where count deltas are being applied onto.
     */
    CountsAccessor.Updater apply( long txId );

    /**
     * Closes this counts store so that no more changes can be made and no more counts can be read.
     */
    @Override
    void close();

    /**
     * Puts the counts store in started state, i.e. after potentially recovery has been made. Any changes {@link #apply(long) applied}
     * before this call is made are considered recovery repairs from a previous non-clean shutdown.
     * @throws IOException any type of error happening when transitioning to started state.
     */
    void start() throws IOException;

    /**
     * Makes a counts store play nice with {@link LifeSupport}. Calls:
     * <ul>
     *     <li>{@link #start()} in {@link Lifecycle#start()}</li>
     *     <li>{@link #close()} in {@link Lifecycle#shutdown()}</li>
     * </ul>
     * @param countsStore {@link CountsStore} to wrap in a {@link Lifecycle}.
     * @return Lifecycle with the wrapped {@link CountsStore} inside of it.
     */
    static Lifecycle wrapInLifecycle( CountsStore countsStore )
    {
        return new LifecycleAdapter()
        {
            @Override
            public void start() throws IOException
            {
                countsStore.start();
            }

            @Override
            public void shutdown()
            {
                countsStore.close();
            }
        };
    }
}
