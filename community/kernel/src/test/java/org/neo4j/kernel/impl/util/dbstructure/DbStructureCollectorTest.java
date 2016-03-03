/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.util.dbstructure;

import org.junit.Test;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Pair.of;

public class DbStructureCollectorTest
{
    @Test
    public void collectsDbStructure()
    {
        // GIVEN
        DbStructureCollector collector = new DbStructureCollector();
        collector.visitLabel( 1, "Person" );
        collector.visitLabel( 2, "City" );
        collector.visitPropertyKey( 1, "name" );
        collector.visitPropertyKey( 2, "income" );
        collector.visitRelationshipType( 1, "LIVES_IN" );
        collector.visitRelationshipType( 2, "FRIEND" );
        collector.visitUniqueIndex( new IndexDescriptor( 1, 1 ), ":Person(name)", 1.0d, 1l );
        collector.visitUniqueConstraint( new UniquenessConstraint( 2, 1 ), ":Person(name)" );
        collector.visitIndex( new IndexDescriptor( 2, 2 ), ":City(income)", 0.2d, 1l );
        collector.visitAllNodesCount( 50 );
        collector.visitNodeCount( 1, "Person", 20 );
        collector.visitNodeCount( 2, "City", 30 );
        collector.visitRelCount( 1, 2, -1, "(:Person)-[:FRIEND]->()", 500 );

        // WHEN
        DbStructureLookup lookup = collector.lookup();

        // THEN
        assertEquals( asList( of( 1, "Person" ), of( 2, "City" ) ), Iterators.asList( lookup.labels() ) );
        assertEquals( asList( of( 1, "name" ), of( 2, "income" ) ), Iterators.asList( lookup.properties() ) );
        assertEquals( asList( of( 1, "LIVES_IN" ), of( 2, "FRIEND" ) ), Iterators.asList( lookup.relationshipTypes() ) );

        assertEquals( asList( of( "City", "name" ) ), Iterators.asList( lookup.knownUniqueConstraints() ) );
        assertEquals( asList( of( "Person", "name" ) ), Iterators.asList( lookup.knownUniqueIndices() ) );
        assertEquals( asList( of( "City", "income" ) ), Iterators.asList( lookup.knownIndices() ) );

        assertEquals( 50, lookup.nodesWithLabelCardinality( -1 ) );
        assertEquals( 20, lookup.nodesWithLabelCardinality( 1 ) );
        assertEquals( 30, lookup.nodesWithLabelCardinality( 2 ) );
        assertEquals( 500, lookup.cardinalityByLabelsAndRelationshipType( 1, 2, -1 ) );
        assertEquals( 1.0d, lookup.indexSelectivity( 1, 1 ), 0.01d );
        assertEquals( 0.2d, lookup.indexSelectivity( 2, 2 ), 0.01d );
    }
}
