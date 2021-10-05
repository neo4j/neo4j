/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.storageengine.api;

import java.util.Optional;

import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.util.Bits;

import static java.lang.String.format;

public interface StoreVersion
{
    String UNKNOWN_VERSION = "Unknown";

    /**
     * @return store version, e.g. as in format version.
     */
    String storeVersion();

    boolean hasCapability( Capability capability );

    boolean hasCompatibleCapabilities( StoreVersion otherVersion, CapabilityType type );

    /**
     * @return the neo4j version where this format was introduced. It is almost certainly NOT the only version of
     * neo4j where this format is used.
     */
    String introductionNeo4jVersion();

    /**
     * @return if this version isn't the latest version the returned optional will contain the store version of the store version superseding this version.
     */
    Optional<String> successorStoreVersion();

    /**
     * @return the latest store version within the same version history line as this store version.
     */
    String latestStoreVersion();

    boolean isCompatibleWith( StoreVersion otherVersion );

    /*
     * The following two methods encode and decode a string that is presumably
     * the store version into a long via Latin1 encoding. This leaves room for
     * 7 characters and 1 byte for the length.
     */
    static long versionStringToLong( String storeVersion )
    {
        if ( UNKNOWN_VERSION.equals( storeVersion ) )
        {
            return -1;
        }
        Bits bits = Bits.bits( 8 );
        int length = storeVersion.length();
        if ( length == 0 || length > 7 )
        {
            throw new IllegalArgumentException( format(
                    "The given string %s is not of proper size for a store version string", storeVersion ) );
        }
        bits.put( length, 8 );
        for ( int i = 0; i < length; i++ )
        {
            char c = storeVersion.charAt( i );
            if ( c >= 256 )
            {
                throw new IllegalArgumentException( format(
                        "Store version strings should be encode-able as Latin1 - %s is not", storeVersion ) );
            }
            bits.put( c, 8 ); // Just the lower byte
        }
        return bits.getLong();
    }

    static String versionLongToString( long storeVersion )
    {
        if ( storeVersion == -1 )
        {
            return UNKNOWN_VERSION;
        }
        Bits bits = Bits.bitsFromLongs( new long[]{storeVersion} );
        int length = bits.getShort( 8 );
        if ( length == 0 || length > 7 )
        {
            throw new IllegalArgumentException( format( "The read version string length %d is not proper.",
                    length ) );
        }
        char[] result = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            result[i] = (char) bits.getShort( 8 );
        }
        return new String( result );
    }
}
