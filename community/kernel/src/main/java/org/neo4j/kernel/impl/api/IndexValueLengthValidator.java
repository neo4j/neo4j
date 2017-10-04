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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.util.Validator;

public class IndexValueLengthValidator implements Validator<byte[]>
{
    // Maximum bytes value length that supported by indexes.
    // Absolute hard maximum length for a term, in bytes once
    // encoded as UTF8.  If a term arrives from the analyzer
    // longer than this length, an IllegalArgumentException
    // when lucene writer trying to add or update document
    static final int MAX_TERM_LENGTH = (1 << 15) - 2;

    public static final IndexValueLengthValidator INSTANCE = new IndexValueLengthValidator();

    private IndexValueLengthValidator()
    {
    }

    @Override
    public void validate( byte[] bytes )
    {
        if ( bytes.length > MAX_TERM_LENGTH )
        {
            throw new IllegalArgumentException( "Property value bytes length: " + bytes.length +
                    " is longer then " + MAX_TERM_LENGTH + ", which is maximum supported length" +
                    " of indexed property value." );
        }
    }
}
