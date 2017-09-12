/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.explicitindex;

/**
 * Abstract interface for accessing legacy auto indexing facilities for nodes and relationships
 *
 * @see AutoIndexOperations
 * @see org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing
 */
public interface AutoIndexing
{
    /**
     * An instance of {@link AutoIndexing} that only returns {@link AutoIndexOperations}
     * that throw {@link UnsupportedOperationException} when any method is invoked on them.
     */
    AutoIndexing UNSUPPORTED = new AutoIndexing()
    {
        @Override
        public AutoIndexOperations nodes()
        {
            return AutoIndexOperations.UNSUPPORTED;
        }

        @Override
        public AutoIndexOperations relationships()
        {
            return AutoIndexOperations.UNSUPPORTED;
        }
    };

    /**
     * @return {@link AutoIndexOperations} for nodes
     */
    AutoIndexOperations nodes();

    /**
     * @return {@link AutoIndexOperations} for relationships
     */
    AutoIndexOperations relationships();
}
