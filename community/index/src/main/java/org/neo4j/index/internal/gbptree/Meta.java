/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.PointerChecking.checkOutOfBounds;
import static org.neo4j.util.Preconditions.checkState;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.index.internal.gbptree.TreeNodeSelector.Factory;
import org.neo4j.io.pagecache.PageCursor;

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
 *       │    │    │    └──────────── {@link #getDataFormatIdentifier()}
 *       │    │    └───────────────── {@link #getDataFormatVersion()}
 *       │    └────────────────────── {@link #getRootFormatIdentifier()}
 *       └─────────────────────────── {@link #getRootFormatVersion()}
 * </pre>
 *
 * {@link #CURRENT_STATE_VERSION} and {@link #CURRENT_GBPTREE_VERSION} aren't used yet because they have
 * never needed to be versioned yet, but remain reserved for future use. The are fixed at 0 a.t.m.
 */
public class Meta {
    static final byte CURRENT_STATE_VERSION = 0;
    static final byte CURRENT_GBPTREE_VERSION = 0;
    public static final int META_SIZE = Integer.BYTES // format
            + Integer.BYTES // payloadSize
            + Long.BYTES // dataLayoutIdentifier
            + Integer.BYTES // dataLayoutMajorVersion
            + Integer.BYTES // dataLayoutMinorVersion
            + Long.BYTES // rootLayoutIdentifier
            + Integer.BYTES // rootLayoutMajorVersion
            + Integer.BYTES; // rootLayoutMinorVersion

    private static final int MASK_BYTE = 0xFF;

    private static final int SHIFT_DATA_FORMAT_IDENTIFIER = Byte.SIZE * 0;
    private static final int SHIFT_DATA_FORMAT_VERSION = Byte.SIZE * 1;
    private static final int SHIFT_ROOT_FORMAT_IDENTIFIER = Byte.SIZE * 2;
    private static final int SHIFT_ROOT_FORMAT_VERSION = Byte.SIZE * 3;
    static final byte UNUSED_VERSION = 0;

    private final byte dataFormatIdentifier;
    private final byte dataFormatVersion;
    private final byte rootFormatIdentifier;
    private final byte rootFormatVersion;
    private final int payloadSize;
    private final long dataLayoutIdentifier;
    private final int dataLayoutMajorVersion;
    private final int dataLayoutMinorVersion;
    private final long rootLayoutIdentifier;
    private final int rootLayoutMajorVersion;
    private final int rootLayoutMinorVersion;

    private Meta(
            byte dataFormatIdentifier,
            byte dataFormatVersion,
            byte rootFormatIdentifier,
            byte rootFormatVersion,
            int payloadSize,
            long dataLayoutIdentifier,
            int dataLayoutMajorVersion,
            int dataLayoutMinorVersion,
            long rootLayoutIdentifier,
            int rootLayoutMajorVersion,
            int rootLayoutMinorVersion) {
        this.dataFormatIdentifier = dataFormatIdentifier;
        this.dataFormatVersion = dataFormatVersion;
        this.rootFormatIdentifier = rootFormatIdentifier;
        this.rootFormatVersion = rootFormatVersion;
        this.payloadSize = payloadSize;
        this.dataLayoutIdentifier = dataLayoutIdentifier;
        this.dataLayoutMajorVersion = dataLayoutMajorVersion;
        this.dataLayoutMinorVersion = dataLayoutMinorVersion;
        this.rootLayoutIdentifier = rootLayoutIdentifier;
        this.rootLayoutMajorVersion = rootLayoutMajorVersion;
        this.rootLayoutMinorVersion = rootLayoutMinorVersion;
    }

    static Meta from(
            int payloadSize, Layout<?, ?> dataLayout, Layout<?, ?> rootLayout, TreeNodeSelector treeNodeSelector) {
        var dataFormat = treeNodeSelector.selectByLayout(dataLayout);
        if (rootLayout != null) {
            var rootFormat = treeNodeSelector.selectByLayout(rootLayout);
            return new Meta(
                    dataFormat.formatIdentifier(),
                    dataFormat.formatVersion(),
                    rootFormat.formatIdentifier(),
                    rootFormat.formatVersion(),
                    payloadSize,
                    dataLayout.identifier(),
                    dataLayout.majorVersion(),
                    dataLayout.minorVersion(),
                    rootLayout.identifier(),
                    rootLayout.majorVersion(),
                    rootLayout.minorVersion());
        } else {
            return new Meta(
                    dataFormat.formatIdentifier(),
                    dataFormat.formatVersion(),
                    UNUSED_VERSION,
                    UNUSED_VERSION,
                    payloadSize,
                    dataLayout.identifier(),
                    dataLayout.majorVersion(),
                    dataLayout.minorVersion(),
                    UNUSED_VERSION,
                    UNUSED_VERSION,
                    UNUSED_VERSION);
        }
    }

    private static Meta parseMeta(
            int format,
            int payloadSize,
            long dataLayoutIdentifier,
            int dataLayoutMajorVersion,
            int dataLayoutMinorVersion,
            long rootLayoutIdentifier,
            int rootLayoutMajorVersion,
            int rootLayoutMinorVersion) {
        return new Meta(
                extractIndividualVersion(format, SHIFT_DATA_FORMAT_IDENTIFIER),
                extractIndividualVersion(format, SHIFT_DATA_FORMAT_VERSION),
                extractIndividualVersion(format, SHIFT_ROOT_FORMAT_IDENTIFIER),
                extractIndividualVersion(format, SHIFT_ROOT_FORMAT_VERSION),
                payloadSize,
                dataLayoutIdentifier,
                dataLayoutMajorVersion,
                dataLayoutMinorVersion,
                rootLayoutIdentifier,
                rootLayoutMajorVersion,
                rootLayoutMinorVersion);
    }

    public static Meta read(ByteBuffer buffer) {
        checkState(buffer.remaining() >= META_SIZE, "Not enough data");
        int format = buffer.getInt();
        int payloadSize = buffer.getInt();
        long dataLayoutIdentifier = buffer.getLong();
        int dataLayoutMajorVersion = buffer.getInt();
        int dataLayoutMinorVersion = buffer.getInt();
        long rootLayoutIdentifier = buffer.getLong();
        int rootLayoutMajorVersion = buffer.getInt();
        int rootLayoutMinorVersion = buffer.getInt();

        return parseMeta(
                format,
                payloadSize,
                dataLayoutIdentifier,
                dataLayoutMajorVersion,
                dataLayoutMinorVersion,
                rootLayoutIdentifier,
                rootLayoutMajorVersion,
                rootLayoutMinorVersion);
    }

    /**
     * Reads meta information from the meta page. The layout identifier and its version
     * that the returned {@link Meta} instance will have are the ones read from the page.
     *
     * @param cursor {@link PageCursor} to read meta information from.
     * @return {@link Meta} instance with all meta information.
     * @throws IOException on {@link PageCursor} I/O error.
     */
    static Meta read(PageCursor cursor) throws IOException {
        byte[] buffer = new byte[META_SIZE];
        do {
            cursor.getBytes(buffer);
        } while (cursor.shouldRetry());
        return read(ByteBuffer.wrap(buffer).order(cursor.getByteOrder()));
    }

    public void verify(Layout<?, ?> dataLayout, RootLayerConfiguration<?> rootLayerConfiguration) {
        verify(dataLayout, rootLayerConfiguration.rootLayout(), DefaultTreeNodeSelector.selector());
    }

    public void verify(Layout<?, ?> dataLayout, Layout<?, ?> rootLayout, TreeNodeSelector treeNodeSelector) {
        if (rootLayout != null) {
            Factory rootFormat = treeNodeSelector.selectByLayout(rootLayout);
            if (rootFormat.formatIdentifier() != rootFormatIdentifier
                    || rootFormat.formatVersion() != rootFormatVersion) {
                throw new MetadataMismatchException(format(
                        "Tried to open using root layout not compatible with what tree was created with. "
                                + "Created with formatIdentifier:%d,formatVersion:%d. Opened with formatIdentifier:%d,formatVersion%d",
                        rootFormatIdentifier,
                        rootFormatVersion,
                        rootFormat.formatIdentifier(),
                        rootFormat.formatVersion()));
            }
        } else {
            if (rootFormatIdentifier != UNUSED_VERSION) {
                throw new MetadataMismatchException(
                        "Unexpected version " + rootFormatIdentifier + " for version slot 3");
            }
            if (rootFormatVersion != UNUSED_VERSION) {
                throw new MetadataMismatchException("Unexpected version " + rootFormatVersion + " for version slot 4");
            }
        }

        if (!dataLayout.compatibleWith(dataLayoutIdentifier, dataLayoutMajorVersion, dataLayoutMinorVersion)) {
            throw new MetadataMismatchException(format(
                    "Tried to open using data layout not compatible with "
                            + "what the index was created with. Created with: layoutIdentifier=%d,majorVersion=%d,minorVersion=%d. "
                            + "Opened with layoutIdentifier=%d,majorVersion=%d,minorVersion=%d",
                    dataLayoutIdentifier,
                    dataLayoutMajorVersion,
                    dataLayoutMinorVersion,
                    dataLayout.identifier(),
                    dataLayout.majorVersion(),
                    dataLayout.minorVersion()));
        }
        if (rootLayout != null
                && !rootLayout.compatibleWith(rootLayoutIdentifier, rootLayoutMajorVersion, rootLayoutMinorVersion)) {
            throw new MetadataMismatchException(format(
                    "Tried to open using root layout not compatible with "
                            + "what the index was created with. Created with: layoutIdentifier=%d,majorVersion=%d,minorVersion=%d. "
                            + "Opened with layoutIdentifier=%d,majorVersion=%d,minorVersion=%d",
                    rootLayoutIdentifier,
                    rootLayoutMajorVersion,
                    rootLayoutMinorVersion,
                    rootLayout.identifier(),
                    rootLayout.majorVersion(),
                    rootLayout.minorVersion()));
        }

        Factory dataFormatByLayout = treeNodeSelector.selectByLayout(dataLayout);
        if (dataFormatByLayout.formatIdentifier() != dataFormatIdentifier
                || dataFormatByLayout.formatVersion() != dataFormatVersion) {
            throw new MetadataMismatchException(format(
                    "Tried to open using data layout not compatible with what tree was created with. "
                            + "Created with formatIdentifier:%d,formatVersion:%d. Opened with formatIdentifier:%d,formatVersion%d",
                    dataFormatIdentifier,
                    dataFormatVersion,
                    dataFormatByLayout.formatIdentifier(),
                    dataFormatByLayout.formatVersion()));
        }
    }

    /**
     * Writes meta information to the meta page.
     *
     * @param cursor {@link PageCursor} to read meta information from.
     */
    void write(PageCursor cursor) {
        cursor.putInt(allVersionsCombined());
        cursor.putInt(getPayloadSize());
        cursor.putLong(getDataLayoutIdentifier());
        cursor.putInt(getDataLayoutMajorVersion());
        cursor.putInt(getDataLayoutMinorVersion());
        cursor.putLong(getRootLayoutIdentifier());
        cursor.putInt(getRootLayoutMajorVersion());
        cursor.putInt(getRootLayoutMinorVersion());
        checkOutOfBounds(cursor);
    }

    private static byte extractIndividualVersion(int format, int shift) {
        return (byte) ((format >>> shift) & MASK_BYTE);
    }

    private int allVersionsCombined() {
        return dataFormatIdentifier << SHIFT_DATA_FORMAT_IDENTIFIER
                | dataFormatVersion << SHIFT_DATA_FORMAT_VERSION
                | rootFormatIdentifier << SHIFT_ROOT_FORMAT_IDENTIFIER
                | rootFormatVersion << SHIFT_ROOT_FORMAT_VERSION;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    byte getDataFormatIdentifier() {
        return dataFormatIdentifier;
    }

    byte getDataFormatVersion() {
        return dataFormatVersion;
    }

    byte getRootFormatIdentifier() {
        return rootFormatIdentifier;
    }

    byte getRootFormatVersion() {
        return rootFormatVersion;
    }

    long getDataLayoutIdentifier() {
        return dataLayoutIdentifier;
    }

    int getDataLayoutMajorVersion() {
        return dataLayoutMajorVersion;
    }

    int getDataLayoutMinorVersion() {
        return dataLayoutMinorVersion;
    }

    long getRootLayoutIdentifier() {
        return rootLayoutIdentifier;
    }

    int getRootLayoutMajorVersion() {
        return rootLayoutMajorVersion;
    }

    int getRootLayoutMinorVersion() {
        return rootLayoutMinorVersion;
    }
}
