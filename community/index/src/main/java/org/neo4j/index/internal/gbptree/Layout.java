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

import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;

/**
 * Main point of interaction for customizing a {@link GBPTree}, how its keys and values are represented
 * as bytes and what keys and values contains.
 * <p>
 * Additionally custom meta data can be supplied, which will be persisted in {@link GBPTree}.
 * <p>
 * Rather extend {@link Adapter} as to get standard implementation of e.g. {@link Adapter#toString()}.
 *
 * @param <KEY> type of key
 * @param <VALUE> type of value
 */
public interface Layout<KEY, VALUE> extends Comparator<KEY>
{
    int FIXED_SIZE_KEY = -1;
    int FIXED_SIZE_VALUE = -1;

    /**
     * @return new key instance.
     */
    KEY newKey();

    /**
     * Copies contents of {@code key} to {@code into}.
     *
     * @param key key (left unchanged as part of this call) to copy contents from.
     * @param into key (changed as part of this call) to copy contents into.
     * @return the provided {@code into} instance for convenience.
     */
    KEY copyKey( KEY key, KEY into );

    /**
     * @return new value instance.
     */
    VALUE newValue();

    /**
     * @param key for which to give size.
     * @return size, in bytes, of given key.
     */
    int keySize( KEY key );

    /**
     * @param value for which to give size.
     * @return size, in bytes, of given value.
     */
    int valueSize( VALUE value );

    /**
     * Writes contents of {@code key} into {@code cursor} at its current offset.
     *
     * @param cursor {@link PageCursor} to write into, at current offset.
     * @param key key containing data to write.
     */
    void writeKey( PageCursor cursor, KEY key );

    /**
     * Writes contents of {@code value} into {@code cursor} at its current offset.
     *
     * @param cursor {@link PageCursor} to write into, at current offset.
     * @param value value containing data to write.
     */
    void writeValue( PageCursor cursor, VALUE value );

    /**
     * Reads key contents at {@code cursor} at its current offset into {@code key}.
     * @param cursor {@link PageCursor} to read from, at current offset.
     * @param into key instances to read into.
     * @param keySize size of key to read or {@link #FIXED_SIZE_KEY} if key is fixed size.
     */
    void readKey( PageCursor cursor, KEY into, int keySize );

    /**
     * Reads value contents at {@code cursor} at its current offset into {@code value}.
     * @param cursor {@link PageCursor} to read from, at current offset.
     * @param into value instances to read into.
     * @param valueSize size of key to read or {@link #FIXED_SIZE_VALUE} if value is fixed size.
     */
    void readValue( PageCursor cursor, VALUE into, int valueSize );

    /**
     * Indicate if keys and values are fixed or dynamix size.
     * @return true if keys and values are fixed size, otherwise true.
     */
    boolean fixedSize();
    /**
     * Used as verification when loading an index after creation, to verify that the same layout is used,
     * as the one it was initially created with.
     *
     * @return a long acting as an identifier, written in the header of an index.
     */
    long identifier();

    /**
     * @return major version of layout. Will be compared to version written into meta page when opening index.
     */
    int majorVersion();

    /**
     * @return minor version of layout. Will be compared to version written into meta page when opening index.
     */
    int minorVersion();

    /**
     * Writes meta data specific to this layout instance to {@code cursor} at its current offset.
     *
     * @param cursor {@link PageCursor} to write into, at its current offset.
     */
    default void writeMetaData( PageCursor cursor )
    {   // no meta-data by default
    }

    /**
     * Reads meta data specific to this layout instance from {@code cursor} at its current offset.
     * The read meta data must also be verified against meta data provided in constructor of this Layout.
     * Constructor-provided meta data can be {@code null} to skip this verification.
     * if read meta data doesn't match with the meta data provided in constructor
     * {@link PageCursor#setCursorException(String)} should be called with appropriate error message.
     *
     * @param cursor {@link PageCursor} to read from, at its current offset.
     */
    default void readMetaData( PageCursor cursor )
    {   // no meta-data by default
    }

    /**
     * Utility method for generating an {@link #identifier()}. Generates an 8-byte identifier from a short name
     * plus a 4-byte identifier.
     *
     * @param name name to be part of this identifier, must at most be 4 characters.
     * @param identifier to include into the returned named identifier.
     * @return a long which is a combination of {@code name} and {@code identifier}.
     */
    static long namedIdentifier( String name, int identifier )
    {
        char[] chars = name.toCharArray();
        if ( chars.length > 4 )
        {
            throw new IllegalArgumentException( "Maximum 4 character name, was '" + name + "'" );
        }
        long upperInt = 0;
        for ( char aChar : chars )
        {
            byte byteValue = (byte) (((byte) aChar) ^ ((byte) (aChar >> 8)));
            upperInt <<= 8;
            upperInt |= byteValue & 0xFF;
        }

        return (upperInt << Integer.SIZE) | identifier;
    }

    /**
     * Typically, a layout is compatible with given identifier, major and minor version if
     * <ul>
     * <li>{@code layoutIdentifier == this.identifier()}</li>
     * <li>{@code majorVersion == this.majorVersion()}</li>
     * <li>{@code minorVersion == this.minorVersion()}</li>
     * </ul>
     * <p>
     * When opening a {@link GBPTree tree} to 'use' it, read and write to it, providing a layout with the right compatibility is
     * important because it decides how to read and write entries in the tree.
     *
     * @param layoutIdentifier the stored layout identifier we want to check compatibility against.
     * @param majorVersion the stored major version we want to check compatibility against.
     * @param minorVersion the stored minor version we want to check compatibility against.
     * @return true if this layout is compatible with combination of identifier, major and minor version, false otherwise.
     */
    boolean compatibleWith( long layoutIdentifier, int majorVersion, int minorVersion );

    /**
     * Adapter for {@link Layout}, which contains convenient standard implementations of some methods.
     *
     * @param <KEY> type of key
     * @param <VALUE> type of value
     */
    abstract class Adapter<KEY, VALUE> implements Layout<KEY,VALUE>
    {
        @Override
        public String toString()
        {
            return format( "%s[version:%d.%d, identifier:%d, keySize:%d, valueSize:%d, fixedSize:%b]",
                    getClass().getSimpleName(), majorVersion(), minorVersion(), identifier(),
                    keySize( null ), valueSize( null ), fixedSize() );
        }

        @Override
        public boolean compatibleWith( long layoutIdentifier, int majorVersion, int minorVersion )
        {
            return layoutIdentifier == identifier() && majorVersion == majorVersion() && minorVersion == minorVersion();
        }
    }
}
