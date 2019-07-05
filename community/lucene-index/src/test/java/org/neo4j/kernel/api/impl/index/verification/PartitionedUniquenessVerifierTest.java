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
package org.neo4j.kernel.api.impl.index.verification;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.verification.DuplicateCheckingCollector;
import org.neo4j.kernel.api.impl.schema.verification.PartitionedUniquenessVerifier;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Values;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.api.impl.LuceneTestUtil.valueTupleList;

class PartitionedUniquenessVerifierTest
{
    private final PartitionSearcher searcher1 = mock( PartitionSearcher.class, Answers.RETURNS_DEEP_STUBS );
    private final PartitionSearcher searcher2 = mock( PartitionSearcher .class, Answers.RETURNS_DEEP_STUBS );
    private final PartitionSearcher searcher3 = mock( PartitionSearcher .class, Answers.RETURNS_DEEP_STUBS );

    @Test
    void partitionSearchersAreClosed() throws IOException
    {
        PartitionedUniquenessVerifier verifier = createPartitionedVerifier();

        verifier.close();

        verify( searcher1 ).close();
        verify( searcher2 ).close();
        verify( searcher3 ).close();
    }

    @Test
    void verifyPropertyUpdates() throws Exception
    {
        PartitionedUniquenessVerifier verifier = createPartitionedVerifier();
        NodePropertyAccessor nodePropertyAccessor = mock( NodePropertyAccessor.class );

        verifier.verify( nodePropertyAccessor, new int[]{42}, valueTupleList( "a", "b" ) );

        verifySearchInvocations( searcher1, "a", "b" );
        verifySearchInvocations( searcher2, "a", "b" );
        verifySearchInvocations( searcher3, "a", "b" );
    }

    private PartitionedUniquenessVerifier createPartitionedVerifier()
    {
        return new PartitionedUniquenessVerifier( getSearchers() );
    }

    private List<SearcherReference> getSearchers()
    {
        return Arrays.asList( searcher1, searcher2, searcher3 );
    }

    private static void verifySearchInvocations( PartitionSearcher searcher, Object... values ) throws IOException
    {
        for ( Object value : values )
        {
            verify( searcher.getIndexSearcher() ).search(
                    eq( LuceneDocumentStructure.newSeekQuery( Values.of( value ) ) ),
                    any( DuplicateCheckingCollector.class ) );
        }
    }
}
