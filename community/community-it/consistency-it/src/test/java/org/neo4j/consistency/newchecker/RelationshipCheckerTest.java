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
package org.neo4j.consistency.newchecker;

import org.junit.jupiter.api.Test;

import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.kernel.api.KernelTransaction;

import static org.mockito.ArgumentMatchers.any;

class RelationshipCheckerTest extends CheckerTestBase
{
    private int type;

    @Override
    void initialData( KernelTransaction tx ) throws KernelException
    {
        type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "A" );
    }

    @Test
    void shouldReportSourceNodeNotInUse() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            relationship( relationship, nodeStore.nextId(), node, type, NULL, NULL, NULL, NULL, true, true );
        }

        // when
        check();

        // then
        expect( RelationshipConsistencyReport.class, report -> report.sourceNodeNotInUse( any() ) );
    }

    @Test
    void shouldReportTargetNodeNotInUse() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            relationship( relationship, node, nodeStore.nextId(), type, NULL, NULL, NULL, NULL, true, true );
        }

        // when
        check();

        // then
        expect( RelationshipConsistencyReport.class, report -> report.targetNodeNotInUse( any() ) );
    }

    @Test
    void shouldReportSourceNodeNotInUseWhenAboveHighId() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            relationship( relationship, node + 10, node, type, NULL, NULL, NULL, NULL, true, true );
        }

        // when
        check();

        // then
        expect( RelationshipConsistencyReport.class, report -> report.sourceNodeNotInUse( any() ) );
    }

    @Test
    void shouldReportTargetNodeNotInUseWhenAboveHighId() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            relationship( relationship, node, node + 10, type, NULL, NULL, NULL, NULL, true, true );
        }

        // when
        check();

        // then
        expect( RelationshipConsistencyReport.class, report -> report.targetNodeNotInUse( any() ) );
    }

    @Test
    void shouldReportSourceNodeDoesNotReferenceBack() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node1 = nodePlusCached( nodeStore.nextId(), NULL, relationshipStore.nextId() );
            long node2 = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            relationship( relationship, node1, node2, type, NULL, NULL, NULL, NULL, true, true );
        }

        // when
        check();

        // then
        expect( RelationshipConsistencyReport.class, report -> report.sourceNodeDoesNotReferenceBack( any() ) );
    }

    @Test
    void shouldReportTargetNodeDoesNotReferenceBack() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node1 = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            long node2 = nodePlusCached( nodeStore.nextId(), NULL, relationshipStore.nextId() );
            relationship( relationship, node1, node2, type, NULL, NULL, NULL, NULL, true, true );
        }

        // when
        check();

        // then
        expect( RelationshipConsistencyReport.class, report -> report.targetNodeDoesNotReferenceBack( any() ) );
    }

    @Test
    void shouldReportRelationshipNotFirstInSourceChain() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node1 = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            long node2 = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            relationship( relationship, node1, node2, type, NULL, NULL, NULL, NULL, false, true );
        }

        // when
        check();

        // then
        expect( NodeConsistencyReport.class, report -> report.relationshipNotFirstInSourceChain( any() ) );
    }

    @Test
    void shouldReportRelationshipNotFirstInTargetChain() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node1 = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            long node2 = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            relationship( relationship, node1, node2, type, NULL, NULL, NULL, NULL, true, false );
        }

        // when
        check();

        // then
        expect( NodeConsistencyReport.class, report -> report.relationshipNotFirstInTargetChain( any() ) );
    }

    @Test
    void shouldReportSourceNodeHasNoRelationships() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node1 = nodePlusCached( nodeStore.nextId(), NULL, NULL );
            long node2 = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            relationship( relationship, node1, node2, type, NULL, NULL, NULL, NULL, true, true );
        }

        // when
        check();

        // then
        expect( RelationshipConsistencyReport.class, report -> report.sourceNodeHasNoRelationships( any() ) );
    }

    @Test
    void shouldReportTargetNodeHasNoRelationships() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node1 = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            long node2 = nodePlusCached( nodeStore.nextId(), NULL, NULL );
            relationship( relationship, node1, node2, type, NULL, NULL, NULL, NULL, true, true );
        }

        // when
        check();

        // then
        expect( RelationshipConsistencyReport.class, report -> report.targetNodeHasNoRelationships( any() ) );
    }

    @Test
    void shouldReportRelationshipTypeNotInUse() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long relationship = relationshipStore.nextId();
            long node1 = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            long node2 = nodePlusCached( nodeStore.nextId(), NULL, relationship );
            relationship( relationship, node1, node2, type + 1, NULL, NULL, NULL, NULL, true, true );
        }

        // when
        check();

        // then
        expect( RelationshipConsistencyReport.class, report -> report.relationshipTypeNotInUse( any() ) );
    }

    private void check() throws Exception
    {
        new RelationshipChecker( context(), noMandatoryProperties ).check( LongRange.range( 0, nodeStore.getHighId() ), true, true );
    }
}
