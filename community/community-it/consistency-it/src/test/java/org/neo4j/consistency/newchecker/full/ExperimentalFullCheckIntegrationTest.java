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
package org.neo4j.consistency.newchecker.full;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.checking.full.FullCheckIntegrationTest;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.experimental_consistency_checker_stop_threshold;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;

public class ExperimentalFullCheckIntegrationTest extends FullCheckIntegrationTest
{
    private HashMap<Setting<?>,Object> extraSettings;

    @BeforeEach
    protected void setUp()
    {
        extraSettings = new HashMap<>();
        super.setUp();
    }

    @Override
    protected Map<Setting<?>,Object> getSettings()
    {
        Map<Setting<?>,Object> cfg = new HashMap<>( super.getSettings() );
        cfg.put( GraphDatabaseSettings.experimental_consistency_checker, true );
        cfg.putAll( extraSettings );
        return cfg;
    }

    @Test
    void shouldOnlyReportFirstNodeInconsistencyOnFailFast() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                tx.create( new NodeRecord( next.node(), false, next.relationship(), -1 ) );
                tx.create( new NodeRecord( next.node(), false, next.relationship(), -1 ) );
            }
        } );

        extraSettings.put( experimental_consistency_checker_stop_threshold, 1 );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 1 )
                .andThatsAllFolks();
    }

    @Test
    void shouldOnlyReportFirstRelationshipInconsistenciesOnFailFast() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                tx.create( new RelationshipRecord( next.relationship(), 1, 2, C ) );
                tx.create( new RelationshipRecord( next.relationship(), 1, 2, C ) );
            }
        } );
        extraSettings.put( experimental_consistency_checker_stop_threshold, 1 );

        // when
        ConsistencySummaryStatistics stats = check();

        // then number of relationship inconsistencies may be 1 or 2, because in a fail-fast setting not all failures are necessarily reported
        // before the checker is aborted. The driver for this arose when adding memory-limited testing to the new checker.
        int relationshipInconsistencies = stats.getInconsistencyCountForRecordType( RecordType.RELATIONSHIP );
        assertThat( relationshipInconsistencies ).isIn( 1, 2 );
        assertEquals( stats.getTotalInconsistencyCount(), relationshipInconsistencies );
    }

    @Test
    void shouldReportRelationshipGroupRelationshipDoesNotShareOwner() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                /*
                 *   node -----> groupA
                 *                   |
                 *                   v
                 *   otherNode <--> relB
                 */
                long node = next.node();
                long otherNode = next.node();
                long group = next.relationshipGroup();
                long rel = next.relationship();
                tx.create( new NodeRecord( node, true, group, NO_NEXT_PROPERTY.intValue() ) );
                tx.create( new NodeRecord( otherNode, false, rel, NO_NEXT_PROPERTY.intValue() ) );
                tx.create( new RelationshipRecord( rel, otherNode, otherNode, C ) );
                tx.create( withOwner( withRelationships( new RelationshipGroupRecord( group, C ),
                        rel, rel, rel ), node ) );
                tx.incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, 1 );
                tx.incrementRelationshipCount( ANY_LABEL, C, ANY_LABEL, 1 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_GROUP, 3 )
                .andThatsAllFolks();
    }

    @Test
    void shouldHandleNegativeRelationshipPointers() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                long node = next.node();
                long otherNode = next.node();
                long rel = next.relationship();
                tx.create( new NodeRecord( node, false, rel, NO_NEXT_PROPERTY.intValue() ) );
                tx.create( new NodeRecord( otherNode, false, rel, NO_NEXT_PROPERTY.intValue() ) );

                RelationshipRecord relationship = new RelationshipRecord( rel, node, otherNode, C );
                relationship.setFirstNextRel( -3 ); //Set some negative pointers
                relationship.setFirstPrevRel( -4 );
                relationship.setSecondNextRel( -5 );
                relationship.setSecondPrevRel( -6 );
                tx.create( relationship );

                tx.incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, 1 );
                tx.incrementRelationshipCount( ANY_LABEL, C, ANY_LABEL, 1 );

            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP, 4 )
                .andThatsAllFolks();
    }

    @Test
    void shouldHandleNegativeNodeRelationshipPointer() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                tx.create( new NodeRecord( next.node(), false, -6, NO_NEXT_PROPERTY.intValue() ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 1 )
                .andThatsAllFolks();
    }

    @Test
    void shouldHandleNegativeRelationshipNodePointers() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                tx.create( new RelationshipRecord( next.relationship(), -2, -3, C ) );

                tx.incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, 1 );
                tx.incrementRelationshipCount( ANY_LABEL, C, ANY_LABEL, 1 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP, 2 )
                .andThatsAllFolks();
    }

    @Disabled( "New checker checks the live graph, i.e. anything that can be reached from the used nodes/relationships" )
    @Test
    protected void shouldReportOrphanedNodeDynamicLabelAsNodeInconsistency() throws Exception
    {
        super.shouldReportOrphanedNodeDynamicLabelAsNodeInconsistency();
    }
}
