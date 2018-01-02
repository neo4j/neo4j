/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

import static java.lang.Math.abs;
import static java.lang.String.format;

public class SourceInformationTest
{
    @Test
    public void shouldEncodeAndDecodeInformation() throws Exception
    {
        // GIVEN
        SourceInformation codec = new SourceInformation();
        Random random = ThreadLocalRandom.current();

        // WHEN/THEN
        for ( int i = 0; i < 100; i++ )
        {
            int sourceId = random.nextInt( 0xFFFF + 1 );
            long lineNumber = abs( random.nextLong() ) & SourceInformation.LINE_NUMBER_MASK;

            long encoded = SourceInformation.encodeSourceInformation( sourceId, lineNumber );
            codec.decode( encoded );

            String hint = format( "sourceId:%d, lineNumber:%d --> %d --> sourceId:%d, lineNumber:%d",
                    sourceId, lineNumber, encoded, codec.sourceId, codec.lineNumber );
            assertEquals( hint, sourceId, codec.sourceId );
            assertEquals( hint, lineNumber, codec.lineNumber );
        }
    }
}
