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
package org.neo4j.index.internal.gbptree;

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
 *       │    │    │    └──────────── {@link TreeNode#formatIdentifier()}
 *       │    │    └───────────────── {@link TreeNode#formatVersion()}
 *       │    └────────────────────── {@link #CURRENT_STATE_VERSION}
 *       └─────────────────────────── {@link #CURRENT_GBPTREE_VERSION}
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
    private final int majorVersion;
    private final int minorVersion;

    private Meta( byte formatIdentifier, byte formatVersion, byte unusedVersionSlot3, byte unusedVersionSlot4,
            int pageSize, long layoutIdentifier, int majorVersion, int minorVersion )
    {
        this.formatIdentifier = formatIdentifier;
        this.formatVersion = formatVersion;
        this.unusedVersionSlot3 = unusedVersionSlot3;
        this.unusedVersionSlot4 = unusedVersionSlot4;
        this.pageSize = pageSize;
        this.layoutIdentifier = layoutIdentifier;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    Meta( byte formatIdentifier, byte formatVersion, int pageSize, Layout layout )
    {
        this( formatIdentifier, formatVersion, UNUSED_VERSION, UNUSED_VERSION,
                pageSize, layout.identifier(), layout.majorVersion(), layout.minorVersion() );
    }

    static Meta parseMeta( int format, int pageSize, long layoutIdentifier, int majorVersion, int minorVersion )
    {
        return new Meta( extractIndividualVersion( format, SHIFT_FORMAT_IDENTIFIER ),
                extractIndividualVersion( format, SHIFT_FORMAT_VERSION ),
                extractIndividualVersion( format, SHIFT_UNUSED_VERSION_SLOT_3 ),
                extractIndividualVersion( format, SHIFT_UNUSED_VERSION_SLOT_4 ),
                pageSize, layoutIdentifier, majorVersion, minorVersion );
    }

    private static byte extractIndividualVersion( int format, int shift )
    {
        return (byte) ((format >>> shift) & MASK_BYTE);
    }

    int allVersionsCombined()
    {
        return formatIdentifier >>> SHIFT_FORMAT_IDENTIFIER | formatVersion >>> SHIFT_FORMAT_VERSION;
    }

    public int getPageSize()
    {
        return pageSize;
    }

    byte getFormatIdentifier()
    {
        return formatIdentifier;
    }

    public byte getFormatVersion()
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
        return majorVersion;
    }

    int getLayoutMinorVersion()
    {
        return minorVersion;
    }
}
