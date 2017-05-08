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
package org.neo4j.kernel.impl.store;

import static java.lang.Math.max;

/**
 * Calculates page ids and offset based on record ids.
 */
public class RecordPageLocationCalculator
{
    private RecordPageLocationCalculator()
    {
    }

    /**
     * Calculates the id of the first record of a page.
     *
     * @param pageId page id
     * @param pageSize size of each page
     * @param recordSize size of each record
     * @param numberOfReservedLowIds reserved low ids
     * @return the id of the first record on the page with the given pageId or the the first non-reserved record id.
     */
    public static long firstRecordOnPage( long pageId, int pageSize, int recordSize, int numberOfReservedLowIds )
    {
        return max( numberOfReservedLowIds, pageId * (pageSize / recordSize) );
    }

    /**
     * Calculates which page a record with the given {@code id} should go into.
     *
     * @param id record id
     * @param pageSize size of each page
     * @param recordSize size of each record
     * @return which page the record with the given {@code id} should go into, given the
     * {@code pageSize} and {@code recordSize}.
     */
    public static long pageIdForRecord( long id, int pageSize, int recordSize )
    {
        return id * recordSize / pageSize;
    }

    /**
     * Calculates which offset into the right page (had by {@link #pageIdForRecord(long, int, int)})
     * the given {@code id} lives at.
     *
     * @param id record id
     * @param pageSize size of each page
     * @param recordSize size of each record
     * @return which offset into the right page the given {@code id} lives at, given the
     * {@code pageSize} and {@code recordSize}.
     */
    public static int offsetForId( long id, int pageSize, int recordSize )
    {
        return (int) ((id * recordSize) % pageSize);
    }
}
