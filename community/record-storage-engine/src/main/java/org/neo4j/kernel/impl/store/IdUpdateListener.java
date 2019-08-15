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
package org.neo4j.kernel.impl.store;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;

public interface IdUpdateListener
{
    void markIdAsUsed( IdType idType, IdGenerator idGenerator, long id );

    void markIdAsUnused( IdType idType, IdGenerator idGenerator, long id );

    IdUpdateListener DIRECT = new IdUpdateListener()
    {
        @Override
        public void markIdAsUsed( IdType idType, IdGenerator idGenerator, long id )
        {
            try ( IdGenerator.CommitMarker marker = idGenerator.commitMarker() )
            {
                marker.markUsed( id );
            }
        }

        @Override
        public void markIdAsUnused( IdType idType, IdGenerator idGenerator, long id )
        {
            try ( IdGenerator.CommitMarker marker = idGenerator.commitMarker() )
            {
                marker.markDeleted( id );
            }
        }
    };

    IdUpdateListener IGNORE = new IdUpdateListener()
    {
        @Override
        public void markIdAsUsed( IdType idType, IdGenerator idGenerator, long id )
        {   // no-op
        }

        @Override
        public void markIdAsUnused( IdType idType, IdGenerator idGenerator, long id )
        {   // no-op
        }
    };
}
