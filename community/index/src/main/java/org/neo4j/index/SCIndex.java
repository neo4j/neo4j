/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface SCIndex extends Closeable
{
    public static final String filePrefix = "shortcut.index.";
    public static final String indexFileSuffix = ".bin";
    public static final String metaFileSuffix = ".meta";

    static String indexFileName( String name )
    {
        return filePrefix + name + indexFileSuffix;
    }

    static String metaFileName( String name )
    {
        return filePrefix + name + metaFileSuffix;
    }

    SCIndexDescription getDescription();

    void insert( long[] key, long[] value ) throws IOException;

    void seek( Seeker seeker, List<SCResult> resultList) throws IOException;
}
