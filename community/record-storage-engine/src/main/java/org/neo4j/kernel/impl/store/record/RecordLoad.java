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
package org.neo4j.kernel.impl.store.record;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.InvalidRecordException;

/**
 * Specifies what happens when loading records, based on inUse status.
 *
 * Roughly this is what happens for the different modes:
 * <ul>
 * <li>{@link RecordLoad#CHECK}: Load at least data to determine whether it's in use or not.
 * If in use then record is loaded into target and returns {@code true},
 * otherwise return {@code false}.</li>
 * <li>{@link RecordLoad#NORMAL}: Load at least data to determine whether it's in use or not.
 * if in use then record is loaded into target returns {@code true},
 * otherwise throws {@link InvalidRecordException}.</li>
 * <li>{@link RecordLoad#FORCE}: Loads record data into target regardless of whether record in use or not.
 * Returns whether record is in use or not.
 * <li>{@link RecordLoad#ALWAYS}: always load all record data, even if the record is marked as not in use, but unlike {@link RecordLoad#FORCE} it will
 * throw decoding and out-of-bounds exceptions. Will not throw InvalidRecordExceptions for records that are not in use.</li>
 * </ul>
 *
 * There are also "LENIENT" variants of all those (except #FORCE), with the difference that reading a record which is not the first unit
 * in a multi-unit record won't raise a cursor exception.
 */
public enum RecordLoad
{
    LENIENT_NORMAL( false, true, true ),
    LENIENT_CHECK( false, false, true ),
    LENIENT_ALWAYS( true, false, true ),
    NORMAL( LENIENT_NORMAL ),
    CHECK( LENIENT_CHECK ),
    ALWAYS( LENIENT_ALWAYS ),
    FORCE( true, false, false, false, null );

    /**
     * <ul>
     * <li>{@code true}: read record data even if the record is marked as unused</li>
     * <li>{@code false}: do not read record data for unused record</li>
     * </ul>
     */
    private final boolean alwaysLoad;

    /**
     * <ul>
     * <li>{@code true}: reading unused record will throw {@link InvalidRecordException}</li>
     * <li>{@code false}: reading unused record will simply not load the record</li>
     * </ul>
     */
    private final boolean failOnUnused;

    /**
     * <ul>
     * <li>{@code true}: check and throw cursor exception from most recent record read/write</li>
     * <li>{@code false}: do not check or throw cursor exception from most recent record read/write</li>
     * </ul>
     */
    private final boolean failOnCursorException;

    /**
     * A lenient cursor, e.g. a scan needs a special flag since it may try to read records that aren't the first unit in
     * multi-unit records, with the expectation that the record should simply look like unused instead of throwing exception
     * <ul>
     * <li>{@code true}: reading a non-first record unit for a multi-unit record will simply complete successfully
     * with the record left in a state where it looks unused</li>
     * <li>{@code false}: reading a non-first record unit for a multi-unit record will set cursor exception</li>
     * </ul>
     */
    private final boolean failOnNonFirstUnit;

    /**
     * Accessor for the lenient variant of this {@link RecordLoad}, i.e. the variant with the same parameters,
     * except that {@link #failOnNonFirstUnit()} returns {@code false}.
     */
    private final RecordLoad lenientVariant;

    /**
     * Constructor for lenient variants which will have {@link #failOnNonFirstUnit} set to {@code false}
     * and its {@link #lenientVariant} set to {@code null}, which means it's already a lenient variant and will return
     * itself in {@link #lenient()}
     */
    RecordLoad( boolean alwaysLoad, boolean failOnUnused, boolean failOnCursorException )
    {
        this( alwaysLoad, failOnUnused, failOnCursorException, false, null );
    }

    /**
     * Constructor for strict variants, i.e. has the parameters of the {@code lenientVariant}, except for {@link #failOnNonFirstUnit}
     * which is {@code true}.
     */
    RecordLoad( RecordLoad lenientVariant )
    {
        this( lenientVariant.alwaysLoad, lenientVariant.failOnUnused, lenientVariant.failOnCursorException, true, lenientVariant );
    }

    RecordLoad( boolean alwaysLoad, boolean failOnUnused, boolean failOnCursorException, boolean failOnNonFirstUnit, RecordLoad lenientVariant )
    {
        this.alwaysLoad = alwaysLoad;
        this.failOnUnused = failOnUnused;
        this.failOnCursorException = failOnCursorException;
        this.failOnNonFirstUnit = failOnNonFirstUnit;
        this.lenientVariant = lenientVariant;
    }

    /**
     * Checks whether a record should be fully loaded from {@link PageCursor}, based on inUse status.
     */
    public final boolean shouldLoad( boolean inUse )
    {
        return inUse || alwaysLoad;
    }

    /**
     * Verifies that a record's in use status is in line with the mode, might throw {@link InvalidRecordException}.
     */
    public final boolean verify( AbstractBaseRecord record )
    {
        boolean inUse = record.inUse();
        if ( failOnUnused && !inUse )
        {
            throw new InvalidRecordException( record + " not in use" );
        }
        return shouldLoad( inUse );
    }

    /**
     * Depending on the mode, this will - if a cursor error has been raised on the given {@link PageCursor} - either
     * throw an {@link InvalidRecordException} with the underlying {@link CursorException}, or clear the error condition
     * on the cursor.
     * @param cursor The {@link PageCursor} to be checked for errors.
     */
    public final void clearOrThrowCursorError( PageCursor cursor )
    {
        if ( failOnCursorException )
        {
            try
            {
                cursor.checkAndClearCursorException();
            }
            catch ( CursorException e )
            {
                throw new InvalidRecordException( e );
            }
        }
        else
        {
            // The FORCE mode does not bother with reporting decoding errors...
            // ... but it must still clear them, since the page cursor may be reused to read other records
            cursor.clearCursorException();
        }
    }

    /**
     * Checks the given {@link PageCursor} to see if its out-of-bounds flag has been raised, and returns {@code true} if
     * that is the case <em>and</em> and out-of-bounds condition should be reported up the stack.
     * @param cursor The {@link PageCursor} to check the bounds flag for.
     * @return {@code true} if an out-of-bounds condition should be reported up the stack, {@code false} otherwise.
     */
    public boolean checkForOutOfBounds( PageCursor cursor )
    {
        return cursor.checkAndClearBoundsFlag() && failOnCursorException;
    }

    public boolean failOnNonFirstUnit()
    {
        return failOnNonFirstUnit;
    }

    public RecordLoad lenient()
    {
        return lenientVariant == null ? this : lenientVariant;
    }
}
