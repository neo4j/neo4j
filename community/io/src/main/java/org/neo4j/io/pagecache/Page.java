/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.io.pagecache;

/**
 * A page in the page cache. Always represents a concrete page in memory, and may
 * represent a particular page in a file, if that file-page has been swapped into the
 * page.
 */
public interface Page
{
    /**
     * Get the size of the cache page in bytes.
     *
     * Don't access memory beyond address() + size().
     */
    int size();

    /**
     * Get the memory address of the beginning of the page.
     *
     * Don't access memory beyond address() + size().
     */
    long address();
}
