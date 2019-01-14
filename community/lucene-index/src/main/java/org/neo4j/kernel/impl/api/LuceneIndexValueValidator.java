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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.index.ArrayEncoder;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Validates {@link Value values} that are about to get indexed into a Lucene index.
 * Values passing this validation are OK to commit and apply to a Lucene index.
 */
public class LuceneIndexValueValidator implements Validator<Value>
{
    public static final LuceneIndexValueValidator INSTANCE = new LuceneIndexValueValidator();

    // Maximum bytes value length that supported by indexes.
    // Absolute hard maximum length for a term, in bytes once
    // encoded as UTF8.  If a term arrives from the analyzer
    // longer than this length, an IllegalArgumentException
    // when lucene writer trying to add or update document
    static final int MAX_TERM_LENGTH = (1 << 15) - 2;

    private final IndexTextValueLengthValidator textValueValidator = new IndexTextValueLengthValidator( MAX_TERM_LENGTH );

    private LuceneIndexValueValidator()
    {
    }

    @Override
    public void validate( Value value )
    {
        textValueValidator.validate( value );
        if ( Values.isArrayValue( value ) )
        {
            textValueValidator.validate( ArrayEncoder.encode( value ).getBytes() );
        }
    }

    public void validate( byte[] encodedValue )
    {
        textValueValidator.validate( encodedValue );
    }
}
