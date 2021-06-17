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
package org.neo4j.kernel.impl.newapi;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.newapi.ParallelPartitionedNodeLabelScanTest.MAX_NUMBER_OF_PARTITIONS;

@ExtendWith( SoftAssertionsExtension.class )
public abstract class ParallelPartitionedNodeLabelScanTransactionStateTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    protected static Map<Label,Integer> LABEL_IDS = new HashMap<>();

    @InjectSoftAssertions
    protected SoftAssertions softly;

    @BeforeAll
    protected void writeLabels() throws KernelException
    {
        try ( var tx = beginTransaction() )
        {
            final var tokenWrite = tx.tokenWrite();
            for ( var label : Label.values() )
            {
                LABEL_IDS.put( label, tokenWrite.labelGetOrCreateForName( label.toString() ) );
            }
            tx.commit();
        }
    }

    @Override
    @BeforeEach
    public void clearGraph()
    {
    }

    @Test
    void shouldThrowOnConstructionWithTransactionState( ) throws KernelException
    {
        try ( var tx = beginTransaction() )
        {
            final var write = tx.dataWrite();
            final var session = nodeLabelIndexSession( tx );
            final var labelId = LABEL_IDS.get( Label.FOO );
            write.nodeAddLabel( write.nodeCreate(), labelId );
            softly.assertThatThrownBy( () -> tx.dataRead().nodeLabelScan( session, new TokenPredicate( labelId ), MAX_NUMBER_OF_PARTITIONS ),
                                       "should throw on construction of scan, with transaction state" )
                  .isInstanceOf( IllegalStateException.class )
                  .hasMessage( "Transaction contains changes; PartitionScan is only valid in Read-Only transactions." );
        }
    }

    @Test
    void shouldThrowOnInitialReserveWithTransactionStateAfterConstruction() throws KernelException
    {
        try ( var tx = beginTransaction();
              var cursor = tx.cursors().allocateNodeLabelIndexCursor( tx.cursorContext() ) )
        {
            final var write = tx.dataWrite();
            final var session = nodeLabelIndexSession( tx );
            final var labelId = LABEL_IDS.get( Label.FOO );
            final var wrap = new Object()
            {
                PartitionedScan<NodeLabelIndexCursor> scan;
            };
            softly.assertThatCode( () -> wrap.scan = tx.dataRead().nodeLabelScan( session, new TokenPredicate( labelId ), MAX_NUMBER_OF_PARTITIONS ) )
                  .as( "should not throw on construction, with no transaction state" )
                  .doesNotThrowAnyException();

            write.nodeAddLabel( write.nodeCreate(), labelId );
            softly.assertThatThrownBy( () -> wrap.scan.reservePartition( cursor ),
                                       "should throw on reserving partition, with transaction state" )
                  .isInstanceOf( IllegalStateException.class )
                  .hasMessage( "Transaction contains changes; PartitionScan is only valid in Read-Only transactions." );
        }
    }

    @Test
    void shouldThrowOnReserveWithLaterTransactionStateAfterConstruction() throws KernelException
    {
        // Require at least 2 partitions for this test
        final var atLeast2Partitions = Math.max( MAX_NUMBER_OF_PARTITIONS, 2 );

        try ( var tx = beginTransaction();
              var cursor = tx.cursors().allocateNodeLabelIndexCursor( tx.cursorContext() ) )
        {
            final var write = tx.dataWrite();
            final var session = nodeLabelIndexSession( tx );
            final var labelId = LABEL_IDS.get( Label.FOO );
            final var wrap = new Object()
            {
                PartitionedScan<NodeLabelIndexCursor> scan;
            };
            softly.assertThatCode( () -> wrap.scan = tx.dataRead().nodeLabelScan( session, new TokenPredicate( labelId ), atLeast2Partitions ) )
                  .as( "should not throw on construction, with no transaction state" )
                  .doesNotThrowAnyException();

            if ( wrap.scan.getNumberOfPartitions() < 2 )
            {
                return;
            }

            softly.assertThatCode( () -> wrap.scan.reservePartition( cursor ) )
                  .as( "should not throw on reserve partition, with no transaction state" )
                  .doesNotThrowAnyException();

            write.nodeAddLabel( write.nodeCreate(), labelId );
            softly.assertThatThrownBy( () -> wrap.scan.reservePartition( cursor ),
                                       "should throw on reserving partition, with transaction state" )
                  .isInstanceOf( IllegalStateException.class )
                  .hasMessage( "Transaction contains changes; PartitionScan is only valid in Read-Only transactions." );
        }
    }

    protected TokenReadSession nodeLabelIndexSession( KernelTransaction tx ) throws IndexNotFoundKernelException
    {
        final var indexes = tx.schemaRead().index( SchemaDescriptors.forAnyEntityTokens( EntityType.NODE ) );
        assertThat( indexes.hasNext() ).as( "NLI exists" ).isTrue();
        final var nli = indexes.next();
        softly.assertThat( indexes.hasNext() ).as( "only one NLI exists" ).isFalse();
        return tx.dataRead().tokenReadSession( nli );
    }

    enum Label implements org.neo4j.graphdb.Label
    {
        FOO,
        BAR,
        BAZ,
    }
}
