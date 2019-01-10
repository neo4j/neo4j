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

import org.neo4j.index.internal.gbptree.TreeNodeSelector.Factory;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.PageCursorUtil.checkOutOfBounds;

/**
 * About versioning (i.e. the format version {@code int}):
 * The format version started out as one int controlling the entire version of the tree and its different types of formats.
 * For compatibility reasons this int has been kept but used differently, i.e. split up into four individual versions,
 * one {@code byte} each. These are:
 *
 * <pre>
 *     <------- int ------>
 * msb [ 3 ][ 2 ][ 1 ][ 0 ] lsb
 *       ▲    ▲    ▲    ▲
 *       │    │    │    │
 *       │    │    │    └──────────── {@link #getFormatIdentifier()}
 *       │    │    └───────────────── {@link #getFormatVersion()}
 *       │    └────────────────────── {@link #getUnusedVersionSlot3()}
 *       └─────────────────────────── {@link #getUnusedVersionSlot4()}
 * </pre>
 *
 * {@link #CURRENT_STATE_VERSION} and {@link #CURRENT_GBPTREE_VERSION} aren't used yet because they have
 * never needed to be versioned yet, but remain reserved for future use. The are fixed at 0 a.t.m.
 */
class Meta
{
    static final byte CURRENT_STATE_VERSION = 0;
    static final byte CURRENT_GBPTREE_VERSION = 0;

    private static final int MASK_BYTE = 0xFF;

    private static final int SHIFT_FORMAT_IDENTIFIER = Byte.SIZE * 0;
    private static final int SHIFT_FORMAT_VERSION = Byte.SIZE * 1;
    private static final int SHIFT_UNUSED_VERSION_SLOT_3 = Byte.SIZE * 2;
    private static final int SHIFT_UNUSED_VERSION_SLOT_4 = Byte.SIZE * 3;
    static final byte UNUSED_VERSION = 0;

    private final byte formatIdentifier;
    private final byte formatVersion;
    private final byte unusedVersionSlot3;
    private final byte unusedVersionSlot4;
    private final int pageSize;
    private final long layoutIdentifier;
    private final int layoutMajorVersion;
    private final int layoutMinorVersion;

    private Meta( byte formatIdentifier, byte formatVersion, byte unusedVersionSlot3, byte unusedVersionSlot4,
            int pageSize, long layoutIdentifier, int layoutMajorVersion, int layoutMinorVersion )
    {
        this.formatIdentifier = formatIdentifier;
        this.formatVersion = formatVersion;
        this.unusedVersionSlot3 = unusedVersionSlot3;
        this.unusedVersionSlot4 = unusedVersionSlot4;
        this.pageSize = pageSize;
        this.layoutIdentifier = layoutIdentifier;
        this.layoutMajorVersion = layoutMajorVersion;
        this.layoutMinorVersion = layoutMinorVersion;
    }

    Meta( byte formatIdentifier, byte formatVersion, int pageSize, Layout<?,?> layout )
    {
        this( formatIdentifier, formatVersion, UNUSED_VERSION, UNUSED_VERSION,
                pageSize, layout.identifier(), layout.majorVersion(), layout.minorVersion() );
    }

    private static Meta parseMeta( int format, int pageSize, long layoutIdentifier, int majorVersion, int minorVersion )
    {
        return new Meta( extractIndividualVersion( format, SHIFT_FORMAT_IDENTIFIER ),
                extractIndividualVersion( format, SHIFT_FORMAT_VERSION ),
                extractIndividualVersion( format, SHIFT_UNUSED_VERSION_SLOT_3 ),
                extractIndividualVersion( format, SHIFT_UNUSED_VERSION_SLOT_4 ),
                pageSize, layoutIdentifier, majorVersion, minorVersion );
    }

    /**
     * Reads meta information from the meta page. Reading meta information also involves {@link Layout} in that
     * it can have written layout-specific information to this page too. The layout identifier and its version
     * that the returned {@link Meta} instance will have are the ones read from the page, not retrieved from {@link Layout}.
     *
     * @param cursor {@link PageCursor} to read meta information from.
     * @param layout {@link Layout} instance that will get the opportunity to read layout-specific data from the meta page.
     * {@code layout} is allowed to be {@code null} where it won't be told to read layout-specific data from the meta page.
     * @return {@link Meta} instance with all meta information.
     * @throws IOException on {@link PageCursor} I/O error.
     */
    static Meta read( PageCursor cursor, Layout<?,?> layout ) throws IOException
    {
        int format;
        int pageSize;
        long layoutIdentifier;
        int layoutMajorVersion;
        int layoutMinorVersion;
        try
        {
            do
            {
                format = cursor.getInt();
                pageSize = cursor.getInt();
                layoutIdentifier = cursor.getLong();
                layoutMajorVersion = cursor.getInt();
                layoutMinorVersion = cursor.getInt();
                if ( layout != null )
                {
                    layout.readMetaData( cursor );
                }
            }
            while ( cursor.shouldRetry() );
            checkOutOfBounds( cursor );
            cursor.checkAndClearCursorException();
        }
        catch ( CursorException e )
        {
            throw new MetadataMismatchException( e,
                    "Tried to open, but caught an error while reading meta data. " +
                    "File is expected to be corrupt, try to rebuild." );
        }

        return parseMeta( format, pageSize, layoutIdentifier, layoutMajorVersion, layoutMinorVersion );
    }

    void verify( Layout<?,?> layout )
    {
        if ( unusedVersionSlot3 != Meta.UNUSED_VERSION )
        {
            throw new MetadataMismatchException( "Unexpected version " + unusedVersionSlot3 + " for unused version slot 3" );
        }
        if ( unusedVersionSlot4 != Meta.UNUSED_VERSION )
        {
            throw new MetadataMismatchException( "Unexpected version " + unusedVersionSlot4 + " for unused version slot 4" );
        }

        if ( !layout.compatibleWith( layoutIdentifier, layoutMajorVersion, layoutMinorVersion ) )
        {
            throw new MetadataMismatchException(
                    "Tried to open using layout not compatible with " +
                    "what the index was created with. Created with: layoutIdentifier=%d,majorVersion=%d,minorVersion=%d. " +
                    "Opened with layoutIdentifier=%d,majorVersion=%d,minorVersion=%d",
                    layoutIdentifier, layoutMajorVersion, layoutMinorVersion,
                    layout.identifier(), layout.majorVersion(), layout.minorVersion() );
        }

        Factory formatByLayout = TreeNodeSelector.selectByLayout( layout );
        if ( formatByLayout.formatIdentifier() != formatIdentifier ||
             formatByLayout.formatVersion() != formatVersion )
        {
            throw new MetadataMismatchException( "Tried to open using layout not compatible with what index was created with. " +
                    "Created with formatIdentifier:%d,formatVersion:%d. Opened with formatIdentifier:%d,formatVersion%d",
                    formatIdentifier, formatVersion, formatByLayout.formatIdentifier(), formatByLayout.formatVersion() );
        }
    }

    /**
     * Writes meta information to the meta page. Writing meta information also involves {@link Layout} in that
     * it can write layout-specific information to this page too.
     *
     * @param cursor {@link PageCursor} to read meta information from.
     * @param layout {@link Layout} instance that will get the opportunity to write layout-specific data to the meta page.
     */
    void write( PageCursor cursor, Layout<?,?> layout )
    {
        cursor.putInt( allVersionsCombined() );
        cursor.putInt( getPageSize() );
        cursor.putLong( getLayoutIdentifier() );
        cursor.putInt( getLayoutMajorVersion() );
        cursor.putInt( getLayoutMinorVersion() );
        layout.writeMetaData( cursor );
        checkOutOfBounds( cursor );
    }

    private static byte extractIndividualVersion( int format, int shift )
    {
        return (byte) ((format >>> shift) & MASK_BYTE);
    }

    private int allVersionsCombined()
    {
        return formatIdentifier << SHIFT_FORMAT_IDENTIFIER | formatVersion << SHIFT_FORMAT_VERSION;
    }

    int getPageSize()
    {
        return pageSize;
    }

    byte getFormatIdentifier()
    {
        return formatIdentifier;
    }

    byte getFormatVersion()
    {
        return formatVersion;
    }

    byte getUnusedVersionSlot3()
    {
        return unusedVersionSlot3;
    }

    byte getUnusedVersionSlot4()
    {
        return unusedVersionSlot4;
    }

    long getLayoutIdentifier()
    {
        return layoutIdentifier;
    }

    int getLayoutMajorVersion()
    {
        return layoutMajorVersion;
    }

    int getLayoutMinorVersion()
    {
        return layoutMinorVersion;
    }
}
