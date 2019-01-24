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
package org.neo4j.unsafe.impl.batchimport.input;

/**
 * Collects items and is {@link #close() closed} after any and all items have been collected.
 * The {@link Collector} is responsible for closing whatever closeable resource received from the importer.
 */
public interface Collector extends AutoCloseable
{
    void collectBadRelationship(
            Object startId, String startIdGroup, String type,
            Object endId, String endIdGroup, Object specificValue );

    void collectDuplicateNode( Object id, long actualId, String group );

    void collectExtraColumns( String source, long row, String value );

    long badEntries();

    boolean isCollectingBadRelationships();

    /**
     * Flushes whatever changes to the underlying resource supplied from the importer.
     */
    @Override
    void close();

    Collector EMPTY = new Collector()
    {
        @Override
        public void collectExtraColumns( String source, long row, String value )
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public long badEntries()
        {
            return 0;
        }

        @Override
        public void collectBadRelationship( Object startId, String startIdGroup, String type, Object endId, String endIdGroup,
                Object specificValue )
        {
        }

        @Override
        public void collectDuplicateNode( Object id, long actualId, String group )
        {
        }

        @Override
        public boolean isCollectingBadRelationships()
        {
            return true;
        }
    };
}
