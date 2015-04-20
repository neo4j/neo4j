/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking;

import org.junit.Test;

import org.neo4j.kernel.api.index.SchemaIndexProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import static org.neo4j.kernel.impl.store.UniquenessConstraintRule.uniquenessConstraintRule;
import static org.neo4j.kernel.impl.store.record.IndexRule.constraintIndexRule;
import static org.neo4j.kernel.impl.store.record.IndexRule.indexRule;

public class SchemaRuleContentTest
{
    @Test
    public void shouldReportIndexRulesWithSameLabelAndPropertyAsEqual() throws Exception
    {
        // given
        SchemaIndexProvider.Descriptor descriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );
        SchemaRuleContent rule1 = new SchemaRuleContent( indexRule( 0, 1, 2, descriptor ) );
        SchemaRuleContent rule2 = new SchemaRuleContent( indexRule( 1, 1, 2, descriptor ) );

        // then
        assertReflectivelyEquals( rule1, rule2 );
    }

    @Test
    public void shouldReportConstraintIndexRulesWithSameLabelAndPropertyAsEqual() throws Exception
    {
        // given
        SchemaIndexProvider.Descriptor descriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );
        SchemaRuleContent rule1 = new SchemaRuleContent( constraintIndexRule( 0, 1, 2, descriptor, null ) );
        SchemaRuleContent rule2 = new SchemaRuleContent( constraintIndexRule( 1, 1, 2, descriptor, 4l ) );

        // then
        assertReflectivelyEquals( rule1, rule2 );
    }

    @Test
    public void shouldReportUniquenessConstraintRulesWithSameLabelAndPropertyAsEqual() throws Exception
    {
        // given
        SchemaRuleContent rule1 = new SchemaRuleContent( uniquenessConstraintRule( 0, 1, 2, 0 ) );
        SchemaRuleContent rule2 = new SchemaRuleContent( uniquenessConstraintRule( 1, 1, 2, 0 ) );

        // then
        assertReflectivelyEquals( rule1, rule2 );
    }

    @Test
    public void shouldReportIndexRulesWithSameLabelButDifferentPropertyAsNotEqual() throws Exception
    {
        // given
        SchemaIndexProvider.Descriptor descriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );
        SchemaRuleContent rule1 = new SchemaRuleContent( indexRule( 0, 1, 2, descriptor ) );
        SchemaRuleContent rule2 = new SchemaRuleContent( indexRule( 1, 1, 3, descriptor ) );

        // then
        assertReflectivelyNotEquals( rule1, rule2 );
    }

    @Test
    public void shouldReportIndexRulesWithSamePropertyButDifferentLabelAsNotEqual() throws Exception
    {
        // given
        SchemaIndexProvider.Descriptor descriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );
        SchemaRuleContent rule1 = new SchemaRuleContent( indexRule( 0, 1, 2, descriptor ) );
        SchemaRuleContent rule2 = new SchemaRuleContent( indexRule( 1, 4, 2, descriptor ) );

        // then
        assertReflectivelyNotEquals( rule1, rule2 );
    }

    @Test
    public void shouldReportIndexRuleAndUniquenessConstraintAsNotEqual() throws Exception
    {
        // given
        SchemaIndexProvider.Descriptor descriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );
        SchemaRuleContent rule1 = new SchemaRuleContent( indexRule( 0, 1, 2, descriptor ) );
        SchemaRuleContent rule2 = new SchemaRuleContent( uniquenessConstraintRule( 1, 1, 2, 0 ) );

        // then
        assertReflectivelyNotEquals( rule1, rule2 );
    }

    @Test
    public void shouldReportConstraintIndexRuleAndUniquenessConstraintAsNotEqual() throws Exception
    {
        // given
        SchemaIndexProvider.Descriptor descriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );
        SchemaRuleContent rule1 = new SchemaRuleContent( constraintIndexRule( 0, 1, 2, descriptor, null ) );
        SchemaRuleContent rule2 = new SchemaRuleContent( uniquenessConstraintRule( 1, 1, 2, 0 ) );

        // then
        assertReflectivelyNotEquals( rule1, rule2 );
    }

    @Test
    public void shouldReportIndexRuleAndUniqueIndexRuleWithSameLabelAndPropertyAsEqual() throws Exception
    {
        // given
        SchemaIndexProvider.Descriptor descriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );
        SchemaRuleContent rule1 = new SchemaRuleContent( indexRule( 0, 1, 2, descriptor ) );
        SchemaRuleContent rule2 = new SchemaRuleContent( constraintIndexRule( 1, 1, 2, descriptor, null ) );

        // then
        assertReflectivelyEquals( rule1, rule2 );
    }

    private static void assertReflectivelyEquals( Object lhs, Object rhs )
    {
        assertEquals( "lhs should be equal to rhs", lhs, rhs );
        assertEquals( "rhs should be equal to lhs", rhs, lhs );
        assertEquals( "hash codes should be equal", lhs.hashCode(), rhs.hashCode() );
    }

    private static void assertReflectivelyNotEquals( Object lhs, Object rhs )
    {
        assertNotEquals( "lhs should not be equal to rhs", lhs, rhs );
        assertNotEquals( "rhs should not be equal to lhs", rhs, lhs );
    }
}
