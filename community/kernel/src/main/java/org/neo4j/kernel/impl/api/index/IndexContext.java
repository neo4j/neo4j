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

import static org.neo4j.helpers.FutureAdapter.VOID;

import java.util.concurrent.Future;

import org.neo4j.kernel.api.InternalIndexState;

/**
 * Controls access to {@link IndexPopulator}, {@link IndexWriter} during different stages
 * of a lifecycle of a schema index. It's designed to be decorated with multiple stacked instances.
 */
public interface IndexContext
{
    void create();
    
    void update( Iterable<NodePropertyUpdate> updates );
    
    /**
     * Initiates dropping this index context. The returned {@link Future} can be used to await
     * its completion.
     * Must close the context as well.
     */
    Future<Void> drop();

    /**
     * Initiates a closing of this index context. The returned {@link Future} can be used to await
     * its completion.
     */
    Future<Void> close();

    InternalIndexState getState();

    void force();
    
    public static class Adapter implements IndexContext
    {
        public static final Adapter EMPTY = new Adapter();

        @Override
        public void create()
        {
        }

        @Override
        public void update( Iterable<NodePropertyUpdate> updates )
        {
        }

        @Override
        public Future<Void> drop()
        {
            return VOID;
        }

        @Override
        public InternalIndexState getState()
        {
            throw new UnsupportedOperationException(  );
        }

        @Override
        public void force()
        {
        }

        @Override
        public Future<Void> close()
        {
            return VOID;
        }
    }
}
