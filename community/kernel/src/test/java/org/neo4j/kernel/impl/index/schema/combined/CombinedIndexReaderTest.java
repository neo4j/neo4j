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
package org.neo4j.kernel.impl.index.schema.combined;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CombinedIndexReaderTest
{
    private IndexReader boostReader;
    private IndexReader fallbackReader;
    private CombinedIndexReader combinedIndexReader;

    @Before
    public void setup()
    {
        boostReader = mock( IndexReader.class );
        fallbackReader = mock( IndexReader.class );
        combinedIndexReader = new CombinedIndexReader( boostReader, fallbackReader );
    }

    /* close */

    @Test
    public void closeMustCloseBothBoostAndFallback() throws Exception
    {
        // when
        combinedIndexReader.close();

        // then
        verify( boostReader, times( 1 ) ).close();
        verify( fallbackReader, times( 1 ) ).close();
    }

    /* countIndexedNodes */

    @Test
    public void countIndexedNodesMustSelectCorrectReader() throws Exception
    {
        // given
        Value[] boostValues = CombinedIndexTestHelp.valuesSupportedByBoost();
        Value[] otherValues = CombinedIndexTestHelp.valuesNotSupportedByBoost();
        Value[] allValues = CombinedIndexTestHelp.allValues();

        // when
        for ( Value boostValue : boostValues )
        {
            verifyCountIndexedNodesWithCorrectReader( boostReader, fallbackReader, boostValue );
        }

        for ( Value otherValue : otherValues )
        {
            verifyCountIndexedNodesWithCorrectReader( fallbackReader, boostReader, otherValue );
        }

        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyCountIndexedNodesWithCorrectReader( fallbackReader, boostReader, firstValue, secondValue );
            }
        }
    }

    private void verifyCountIndexedNodesWithCorrectReader( IndexReader correct, IndexReader wrong, Value... boostValue )
    {
        combinedIndexReader.countIndexedNodes( 0, boostValue );
        verify( correct, times( 1 ) ).countIndexedNodes( 0, boostValue );
        verify( wrong, times( 0 ) ).countIndexedNodes( 0, boostValue );
    }

    /* query */
    // todo ...
}
