/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;

@FunctionalInterface
public
interface IndexDropAction
{
    /**
     * Deletes the index directory and everything in it, as last part of dropping an index.
     * Can be configured to create archive with content of index directories for future analysis.
     *
     * @param indexId the index id, for which directory to drop.
     * @param archiveExistentIndex create archive with content of dropped directories
     * @see GraphDatabaseSettings#archive_failed_index
     */
    void drop( long indexId, boolean archiveExistentIndex );

    /**
     * Deletes the index directory and everything in it, as last part of dropping an index.
     *
     * @param indexId the index id, for which directory to drop.
     */
    default void drop( long indexId )
    {
        drop( indexId, false );
    }
}
