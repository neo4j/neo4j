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
package org.neo4j.kernel.impl.util.dbstructure;

import org.junit.Test;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
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
        collector.visitIndex( SchemaIndexDescriptorFactory.uniqueForLabel( 1, 1 ), ":Person(name)", 1.0d, 1L );
        collector.visitUniqueConstraint( ConstraintDescriptorFactory.uniqueForLabel( 2, 1 ), ":City(name)" );
        collector.visitNodeKeyConstraint( ConstraintDescriptorFactory.nodeKeyForLabel( 2, 1 ), ":City(name)" );
        collector.visitIndex( SchemaIndexDescriptorFactory.forLabel( 2, 2 ), ":City(income)", 0.2d, 1L );
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

        assertEquals( asList( "Person" ),
                Iterators.asList( Iterators.map( Pair::first, lookup.knownUniqueIndices() ) ) );
        assertArrayEquals( new String[]{"name"}, lookup.knownUniqueIndices().next().other() );

        assertEquals( asList( "City" ),
                Iterators.asList( Iterators.map( Pair::first, lookup.knownNodeKeyConstraints() ) ) );
        assertArrayEquals( new String[]{"name"}, lookup.knownNodeKeyConstraints().next().other() );

        assertEquals( asList( "City" ),
                Iterators.asList( Iterators.map( Pair::first, lookup.knownUniqueConstraints() ) ) );
        assertArrayEquals( new String[]{"name"}, lookup.knownUniqueConstraints().next().other() );

        assertEquals( asList( "City" ), Iterators.asList( Iterators.map( Pair::first, lookup.knownIndices() ) ) );
        assertArrayEquals( new String[]{"income"}, lookup.knownIndices().next().other() );

        assertEquals( 50, lookup.nodesAllCardinality() );
        assertEquals( 20, lookup.nodesWithLabelCardinality( 1 ) );
        assertEquals( 30, lookup.nodesWithLabelCardinality( 2 ) );
        assertEquals( 500, lookup.cardinalityByLabelsAndRelationshipType( 1, 2, -1 ) );
        assertEquals( 1.0d, lookup.indexSelectivity( 1, 1 ), 0.01d );
        assertEquals( 0.2d, lookup.indexSelectivity( 2, 2 ), 0.01d );
    }

    @Test
    public void collectsCompositeDbStructure()
    {
        // GIVEN
        DbStructureCollector collector = new DbStructureCollector();
        collector.visitLabel( 1, "Person" );
        collector.visitLabel( 2, "City" );
        collector.visitPropertyKey( 1, "name" );
        collector.visitPropertyKey( 2, "income" );
        collector.visitPropertyKey( 3, "lastName" );
        collector.visitPropertyKey( 4, "tax" );
        collector.visitPropertyKey( 5, "area" );
        collector.visitRelationshipType( 1, "LIVES_IN" );
        collector.visitRelationshipType( 2, "FRIEND" );
        collector.visitIndex( SchemaIndexDescriptorFactory.uniqueForLabel( 1, 1, 3 ), ":Person(name, lastName)", 1.0d, 1L );
        collector.visitUniqueConstraint( ConstraintDescriptorFactory.uniqueForLabel( 2, 1, 5 ), ":City(name, area)" );
        collector.visitIndex( SchemaIndexDescriptorFactory.forLabel( 2, 2, 4 ), ":City(income, tax)", 0.2d, 1L );
        collector.visitAllNodesCount( 50 );
        collector.visitNodeCount( 1, "Person", 20 );
        collector.visitNodeCount( 2, "City", 30 );
        collector.visitRelCount( 1, 2, -1, "(:Person)-[:FRIEND]->()", 500 );

        // WHEN
        DbStructureLookup lookup = collector.lookup();

        // THEN
        assertEquals( asList( of( 1, "Person" ), of( 2, "City" ) ), Iterators.asList( lookup.labels() ) );
        assertEquals(
                asList( of( 1, "name" ), of( 2, "income" ), of( 3, "lastName" ), of( 4, "tax" ), of( 5, "area" ) ),
                Iterators.asList( lookup.properties() ) );
        assertEquals( asList( of( 1, "LIVES_IN" ), of( 2, "FRIEND" ) ),
                Iterators.asList( lookup.relationshipTypes() ) );

        assertEquals( asList( "Person" ),
                Iterators.asList( Iterators.map( Pair::first, lookup.knownUniqueIndices() ) ) );
        assertArrayEquals( new String[]{"name", "lastName"}, lookup.knownUniqueIndices().next().other() );
        assertEquals( asList( "City" ),
                Iterators.asList( Iterators.map( Pair::first, lookup.knownUniqueConstraints() ) ) );
        assertArrayEquals( new String[]{"name", "area"}, lookup.knownUniqueConstraints().next().other() );
        assertEquals( asList( "City" ), Iterators.asList( Iterators.map( Pair::first, lookup.knownIndices() ) ) );
        assertArrayEquals( new String[]{"income", "tax"}, lookup.knownIndices().next().other() );

        assertEquals( 50, lookup.nodesAllCardinality() );
        assertEquals( 20, lookup.nodesWithLabelCardinality( 1 ) );
        assertEquals( 30, lookup.nodesWithLabelCardinality( 2 ) );
        assertEquals( 500, lookup.cardinalityByLabelsAndRelationshipType( 1, 2, -1 ) );
        assertEquals( 1.0d, lookup.indexSelectivity( 1, 1, 3 ), 0.01d );
        assertEquals( 0.2d, lookup.indexSelectivity( 2, 2, 4 ), 0.01d );
    }
}
