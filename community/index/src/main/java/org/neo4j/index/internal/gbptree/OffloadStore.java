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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;

public interface OffloadStore<KEY, VALUE>
{
    int maxEntrySize();

    void readKey( long offloadId, KEY into ) throws IOException;

    void readKeyValue( long offloadId, KEY key, VALUE value ) throws IOException;

    void readValue( long offloadId, VALUE into ) throws IOException;

    long writeKey( KEY key, long stableGeneration, long unstableGeneration ) throws IOException;

    long writeKeyValue( KEY key, VALUE value, long stableGeneration, long unstableGeneration ) throws IOException;

    void free( long offloadId );
}
