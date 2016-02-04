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
package org.neo4j.kernel.impl.store.record;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.InvalidRecordException;

/**
 * Specifies what happens when loading records, based on inUse status.
 *
 * Roughly this is what happens for the different modes:
 * <ul>
 * <li>{@link RecordLoad#CHECK}: Load at least data to determine whether or not it's in use.
 * If in use then record is loaded into target and returns {@code true},
 * otherwise return {@code false}.</li>
 * <li>{@link RecordLoad#NORMAL}: Load at least data to determine whether or not it's in use.
 * if in use then record is loaded into target returns {@code true},
 * otherwise throws {@link InvalidRecordException}.</li>
 * <li>{@link RecordLoad#FORCE}: Loads record data into target regardless of whether or not record in use.
 * Returns whether or not record is in use.
 *
 */
public enum RecordLoad
{
    NORMAL
    {
        @Override
        public boolean shouldLoad( boolean inUse )
        {
            return inUse;
        }

        @Override
        public boolean verify( AbstractBaseRecord record )
        {
            if ( !record.inUse() )
            {
                throw new InvalidRecordException( record + " not in use" );
            }
            return true;
        }
    },
    CHECK
    {
        @Override
        public boolean shouldLoad( boolean inUse )
        {
            return inUse;
        }

        @Override
        public boolean verify( AbstractBaseRecord record )
        {
            return record.inUse();
        }

        @Override
        public void report( String message )
        {
        }
    },
    FORCE
    {
        @Override
        public boolean shouldLoad( boolean inUse )
        {
            // Always return true so that record data will always be loaded, even if not in use.
            return true;
        }

        @Override
        public boolean verify( AbstractBaseRecord record )
        {
            return true;
        }

        @Override
        public void report( String message )
        {
            // Don't report
        }
    };

    /**
     * Checks whether or not a record should be fully loaded from {@link PageCursor}, based on inUse status.
     */
    public abstract boolean shouldLoad( boolean inUse );

    /**
     * Verifies that a record's in use status is in line with the mode, might throw {@link InvalidRecordException}.
     */
    public abstract boolean verify( AbstractBaseRecord record );

    public void report( String message )
    {
        throw new InvalidRecordException( message );
    }
}
