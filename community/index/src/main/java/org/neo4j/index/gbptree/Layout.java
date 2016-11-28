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
package org.neo4j.index.gbptree;

import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Main point of interaction for customizing a {@link GBPTree}, how its keys and values are represented
 * as bytes and what keys and values contains.
 * <p>
 * Additionally custom meta data can be supplied, which will be persisted in {@link GBPTree}.
 *
 * @param <KEY> type of key
 * @param <VALUE> type of value
 */
public interface Layout<KEY,VALUE> extends Comparator<KEY>
{
    /**
     * @return new key instances.
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
     * @return size, in bytes, of a key.
     */
    int keySize();

    /**
     * @return size, in bytes, of a value.
     */
    int valueSize();

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
     *
     * @param cursor {@link PageCursor} to read from, at current offset.
     * @param into key instances to read into.
     */
    void readKey( PageCursor cursor, KEY into );

    /**
     * Reads value contents at {@code cursor} at its current offset into {@code value}.
     *
     * @param cursor {@link PageCursor} to read from, at current offset.
     * @param into value instances to read into.
     */
    void readValue( PageCursor cursor, VALUE into );

    /**
     * Used as a checksum for when loading an index after creation, to verify that the same layout is used,
     * as the one it was initially created with.
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
    void writeMetaData( PageCursor cursor );

    /**
     * Reads meta data specific to this layout instance from {@code cursor} at its current offset.
     * The read meta data must also be verified against meta data provided in constructor of this Layout.
     * Constructor-provided meta data can be {@code null} to skip this verification.
     *
     * @param cursor {@link PageCursor} to read from, at its current offset.
     * @throws IllegalArgumentException if read meta data doesn't match with the meta data provided in constructor.
     */
    void readMetaData( PageCursor cursor );

    /**
     * Utility method for generating an {@link #identifier()}.
     */
    static long namedIdentifier( String name, int checksum )
    {
        char[] chars = name.toCharArray();
        if ( chars.length > 4 )
        {
            throw new IllegalArgumentException( "Maximum 4 character name, was '" + name + "'" );
        }
        long upperInt = 0;
        for ( int i = 0; i < chars.length; i++ )
        {
            byte byteValue = (byte) (((byte) chars[i]) ^ ((byte) (chars[i] >> 8)));
            upperInt <<= 8;
            upperInt |= (byteValue & 0xFF);
        }

        return upperInt << Integer.SIZE | (checksum & 0xFFFFFFFF);
    }
}
