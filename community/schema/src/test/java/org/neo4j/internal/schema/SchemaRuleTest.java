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
package org.neo4j.internal.schema;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;

class SchemaRuleTest
{
    private IndexConfig indexConfig = IndexConfig.empty();
    private LabelSchemaDescriptor labelSchema = SchemaDescriptor.forLabel( 1, 2, 3 );
    private RelationTypeSchemaDescriptor relTypeSchema = SchemaDescriptor.forRelType( 1, 2, 3 );
    private FulltextSchemaDescriptor fulltextNodeSchema = SchemaDescriptor.fulltext( NODE, indexConfig, new int[]{1, 2}, new int[]{1, 2} );
    private FulltextSchemaDescriptor fulltextRelSchema = SchemaDescriptor.fulltext( RELATIONSHIP, indexConfig, new int[]{1, 2}, new int[]{1, 2} );
    private LabelSchemaDescriptor labelSchema2 = SchemaDescriptor.forLabel( 0, 0, 1 );
    private FulltextSchemaDescriptor fulltextNodeSchema2 = SchemaDescriptor.fulltext( NODE, indexConfig, new int[]{0, 1}, new int[]{0, 1} );
    private IndexPrototype labelPrototype = IndexPrototype.forSchema( labelSchema );
    private IndexPrototype labelUniquePrototype = IndexPrototype.uniqueForSchema( labelSchema );
    private IndexPrototype relTypePrototype = IndexPrototype.forSchema( relTypeSchema );
    private IndexPrototype relTypeUniquePrototype = IndexPrototype.uniqueForSchema( relTypeSchema );
    private IndexPrototype nodeFtsPrototype = IndexPrototype.forSchema( fulltextNodeSchema );
    private IndexPrototype relFtsPrototype = IndexPrototype.forSchema( fulltextRelSchema );
    private IndexPrototype labelPrototype2 = IndexPrototype.forSchema( labelSchema2 );
    private IndexPrototype nodeFtsPrototype2 = IndexPrototype.forSchema( fulltextNodeSchema2 );
    private ConstraintDescriptor uniqueLabelConstraint = ConstraintDescriptorFactory.uniqueForSchema( labelSchema );
    private ConstraintDescriptor existsLabelConstraint = ConstraintDescriptorFactory.existsForSchema( labelSchema );
    private ConstraintDescriptor nodeKeyConstraint = ConstraintDescriptorFactory.nodeKeyForSchema( labelSchema );
    private ConstraintDescriptor existsRelTypeConstraint = ConstraintDescriptorFactory.existsForSchema( relTypeSchema );
    private ConstraintDescriptor uniqueLabelConstraint2 = ConstraintDescriptorFactory.uniqueForSchema( labelSchema2 );
    private List<String> labels = List.of( "La:bel", "Label1", "Label2" );
    private List<String> types = List.of( "Ty:pe", "Type1", "Type2" );
    private List<String> properties = List.of( "prop:erty", "prop1", "prop2", "prop3" );
    private TokenNameLookup lookup = new TokenNameLookup()
    {
        @Override
        public String labelGetName( int labelId )
        {
            return labels.get( labelId );
        }

        @Override
        public String relationshipTypeGetName( int relationshipTypeId )
        {
            return types.get( relationshipTypeId );
        }

        @Override
        public String propertyKeyGetName( int propertyKeyId )
        {
            return properties.get( propertyKeyId );
        }
    };

    @Test
    void mustGenerateReasonableNames()
    {

        assertName( labelPrototype, new String[]{"A"}, new String[]{"B", "C"}, "Index on :A (B,C)" );
        assertName( labelUniquePrototype, new String[]{"A"}, new String[]{"B", "C"}, "Unique Index on :A (B,C)" );
        assertName( relTypePrototype, new String[]{"A"}, new String[]{"B", "C"}, "Index on ()-[:A]-() (B,C)" );
        assertName( relTypeUniquePrototype, new String[]{"A"}, new String[]{"B", "C"}, "Unique Index on ()-[:A]-() (B,C)" );
        assertName( nodeFtsPrototype, new String[]{"A", "B"}, new String[]{"C", "D"}, "Full-Text Index on :A,:B (C,D)" );
        assertName( relFtsPrototype, new String[]{"A", "B"}, new String[]{"C", "D"}, "Full-Text Index on ()-[:A|:B]-() (C,D)" );
        assertName( uniqueLabelConstraint, new String[] {"A"}, new String[] {"B", "C"}, "Uniqueness constraint on :A (B,C)" );
        assertName( existsLabelConstraint, new String[] {"A"}, new String[] {"B", "C"}, "Property existence constraint on :A (B,C)" );
        assertName( nodeKeyConstraint, new String[] {"A"}, new String[] {"B", "C"}, "Node key constraint on :A (B,C)" );
        assertName( existsRelTypeConstraint, new String[] {"A"}, new String[] {"B", "C"}, "Property existence constraint on ()-[:A]-() (B,C)" );
    }

    @Test
    void mustGenerateReasonableUserDescription()
    {
        assertUserDescription( "Index( GENERAL, :Label1(prop2, prop3), Undecided-0 )", labelPrototype );
        assertUserDescription( "Index( UNIQUE, :Label1(prop2, prop3), Undecided-0 )", labelUniquePrototype );
        assertUserDescription( "Index( GENERAL, -[:Type1(prop2, prop3)]-, Undecided-0 )", relTypePrototype );
        assertUserDescription( "Index( UNIQUE, -[:Type1(prop2, prop3)]-, Undecided-0 )", relTypeUniquePrototype );
        assertUserDescription( "Index( GENERAL, :Label1,Label2(prop1, prop2), Undecided-0 )", nodeFtsPrototype );
        assertUserDescription( "Index( GENERAL, -[:Type1,Type2(prop1, prop2)]-, Undecided-0 )", relFtsPrototype );
        assertUserDescription( "Constraint( UNIQUE, :Label1(prop2, prop3) )", uniqueLabelConstraint );
        assertUserDescription( "Constraint( EXISTS, :Label1(prop2, prop3) )", existsLabelConstraint );
        assertUserDescription( "Constraint( UNIQUE_EXISTS, :Label1(prop2, prop3) )", nodeKeyConstraint );
        assertUserDescription( "Constraint( EXISTS, -[:Type1(prop2, prop3)]- )", existsRelTypeConstraint );
        assertUserDescription( "Index( GENERAL, :`La:bel`(`prop:erty`, prop1), Undecided-0 )", labelPrototype2 );
        assertUserDescription( "Index( GENERAL, :`La:bel`,Label1(`prop:erty`, prop1), Undecided-0 )", nodeFtsPrototype2 );
        assertUserDescription( "Constraint( UNIQUE, :`La:bel`(`prop:erty`, prop1) )", uniqueLabelConstraint2 );
    }

    @Test
    void mustNotGenerateBrokenNamesWhenTokensContainNullBytes()
    {
        assertName( IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 2 ) ), new String[]{"a\0b"}, new String[]{"c\0d"}, "Index on :a\\0b (c\\0d)" );
    }

    private void assertName( SchemaDescriptorSupplier schemaish, String[] entityTokenNames, String[] propertyNames, String expectedName )
    {
        String generateName = SchemaRule.generateName( schemaish, entityTokenNames, propertyNames );
        assertEquals( expectedName, generateName );
        assertEquals( expectedName, SchemaRule.sanitiseName( generateName ) );
    }

    private void assertUserDescription( String description, SchemaDescriptorSupplier schemaish )
    {
        assertEquals( description, schemaish.userDescription( lookup ), "wrong userDescription for " + schemaish );
    }
}
