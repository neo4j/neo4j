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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;

class SchemaRuleTest
{
    private LabelSchemaDescriptor labelSchema = SchemaDescriptor.forLabel( 1, 2, 3 );
    private RelationTypeSchemaDescriptor relTypeSchema = SchemaDescriptor.forRelType( 1, 2, 3 );
    private FulltextSchemaDescriptor fulltextNodeSchema = SchemaDescriptor.fulltext( NODE, new int[]{1, 2}, new int[]{1, 2} );
    private FulltextSchemaDescriptor fulltextRelSchema = SchemaDescriptor.fulltext( RELATIONSHIP, new int[]{1, 2}, new int[]{1, 2} );
    private LabelSchemaDescriptor labelSchema2 = SchemaDescriptor.forLabel( 0, 0, 1 );
    private FulltextSchemaDescriptor fulltextNodeSchema2 = SchemaDescriptor.fulltext( NODE, new int[]{0, 1}, new int[]{0, 1} );
    private IndexPrototype labelPrototype = IndexPrototype.forSchema( labelSchema );
    private IndexPrototype labelUniquePrototype = IndexPrototype.uniqueForSchema( labelSchema );
    private IndexPrototype relTypePrototype = IndexPrototype.forSchema( relTypeSchema );
    private IndexPrototype relTypeUniquePrototype = IndexPrototype.uniqueForSchema( relTypeSchema );
    private IndexPrototype nodeFtsPrototype = IndexPrototype.forSchema( fulltextNodeSchema ).withIndexType( IndexType.FULLTEXT );
    private IndexPrototype relFtsPrototype = IndexPrototype.forSchema( fulltextRelSchema ).withIndexType( IndexType.FULLTEXT );
    private IndexPrototype labelPrototype2 = IndexPrototype.forSchema( labelSchema2 ).withIndexType( IndexType.FULLTEXT );
    private IndexPrototype nodeFtsPrototype2 = IndexPrototype.forSchema( fulltextNodeSchema2 ).withIndexType( IndexType.FULLTEXT );
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

    /**
     * There are many tests throughout the code base that end up relying on indexes getting specific names.
     * For that reason, we need to keep the hash function output relatively pinned down.
     */
    @Test
    void mustGenerateDeterministicNames()
    {
        assertName( labelPrototype, "index_41a159fc" );
        assertName( labelUniquePrototype, "index_cc141e85" );
        assertName( relTypePrototype, "index_d0e8fbc6" );
        assertName( relTypeUniquePrototype, "index_918ead01" );
        assertName( nodeFtsPrototype, "index_99c88876" );
        assertName( relFtsPrototype, "index_9c14864e" );
        assertName( uniqueLabelConstraint, "constraint_9d5cc155" );
        assertName( existsLabelConstraint, "constraint_b23c1483" );
        assertName( nodeKeyConstraint, "constraint_7b8dd387" );
        assertName( existsRelTypeConstraint, "constraint_ef4bbcac" );
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

    private void assertName( SchemaDescriptorSupplier schemaish, String expectedName )
    {
        String generateName = SchemaRule.generateName( schemaish, new String[]{"A"}, new String[]{"B", "C"} );
        assertThat( generateName ).isEqualTo( expectedName );
        assertThat( SchemaRule.sanitiseName( generateName ) ).isEqualTo( expectedName );
    }

    private void assertUserDescription( String description, SchemaDescriptorSupplier schemaish )
    {
        assertEquals( description, schemaish.userDescription( lookup ), "wrong userDescription for " + schemaish );
    }

    @SuppressWarnings( {"OptionalAssignedToNull", "ConstantConditions"} )
    @Test
    void sanitiseNameMustRejectEmptyOptionalOrNullNames()
    {
        assertThrows( IllegalArgumentException.class, () -> SchemaRule.sanitiseName( Optional.empty() ) );
        assertThrows( NullPointerException.class, () -> SchemaRule.sanitiseName( (Optional<String>) null ) );
        assertThrows( IllegalArgumentException.class, () -> SchemaRule.sanitiseName( (String) null ) );
    }

    @Test
    void sanitiseNameMustRejectReservedNames()
    {
        Set<String> reservedNames = ReservedSchemaRuleNames.getReservedNames();
        reservedNames = reservedNames.stream().flatMap( n -> Stream.of( " " + n, n, n + " " ) ).collect( Collectors.toSet() );
        for ( String reservedName : reservedNames )
        {
            assertThrows( IllegalArgumentException.class, () -> SchemaRule.sanitiseName( reservedName ), "reserved name: '" + reservedName + "'" );
        }
    }

    @Test
    void sanitiseNameMustRejectInvalidNames()
    {
        List<String> invalidNames = List.of( "", "\0", "`", "``", "`a`", "a`b", "a``b" , " ", "  ", "\t", " \t ", "\n", "\r" );

        for ( String invalidName : invalidNames )
        {
            assertThrows( IllegalArgumentException.class, () -> SchemaRule.sanitiseName( invalidName ), "invalid name: '" + invalidName + "'" );
        }
    }

    @Test
    void sanitiseNameMustAcceptValidNames()
    {
        List<String> validNames = List.of(
                ".", ",", "'", "a", " a", "a ", "a b", "a\n", "a\nb", "\"", "@", "#", "$", "%", "{", "}", "\uD83D\uDE02", ":", ";", "[", "]", "-", "_" );

        for ( String validName : validNames )
        {
            SchemaRule.sanitiseName( validName );
        }
    }
}
