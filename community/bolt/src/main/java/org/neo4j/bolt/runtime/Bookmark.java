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
package org.neo4j.bolt.runtime;

import org.neo4j.kernel.database.NamedDatabaseId;

public interface Bookmark
{
    String BOOKMARK_KEY = "bookmark"; // used in response messages

    long txId();

    NamedDatabaseId databaseId();

    void attachTo( BoltResponseHandler state );

    Bookmark EMPTY_BOOKMARK = new Bookmark()
    {
        @Override
        public long txId()
        {
            throw new UnsupportedOperationException( "Empty bookmark does not have a transaction ID" );
        }

        @Override
        public NamedDatabaseId databaseId()
        {
            throw new UnsupportedOperationException( "Empty bookmark does not have a database ID" );
        }

        @Override
        public void attachTo( BoltResponseHandler state )
        {
            // doing nothing
        }
    };
}
