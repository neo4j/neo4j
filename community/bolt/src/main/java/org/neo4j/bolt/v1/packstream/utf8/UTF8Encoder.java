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
package org.neo4j.bolt.v1.packstream.utf8;

import java.nio.ByteBuffer;

/**
 * A non-thread-safe UTF8 encoding interface, delegates to near-zero GC overhead
 * UTF8 implementations on HotSpot, falls back to stdlib encoder if on other JVM.
 *
 * This implementation solves a major GC bottleneck in that we don't have to
 * allocate objects to encode most strings.
 *
 * We currently do "bulk" encoding, where the whole string is turned
 * into UTF-8 before it gets returned. This is simply a limitation in
 * PackStream currently in that we need to know the length of utf-8
 * strings up-front, so we can't stream them out.
 *
 * This becomes an issue for very large strings, and should be remedied
 * in Bolt V2 by introducing streaming options for Strings in the same
 * manner we've discussed adding streaming lists.
 *
 * Once that is resolved, we could have a method here that took a
 * WritableByteChannel or similar instead.
 */
public interface UTF8Encoder
{
    /**
     * @return a ByteBuffer with the encoded string. This will be overwritten
     *         the next time you call this method, so use it or loose it!
     */
    ByteBuffer encode( String input );

    static UTF8Encoder fastestAvailableEncoder()
    {
        try
        {
            return (UTF8Encoder)Class
                    .forName("org.neo4j.bolt.v1.packstream.utf8.SunMiscUTF8Encoder")
                    .getConstructor()
                    .newInstance();
        }
        catch ( Throwable e )
        {
            return new VanillaUTF8Encoder();
        }
    }

}
