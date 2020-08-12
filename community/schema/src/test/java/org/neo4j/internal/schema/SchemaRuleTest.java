/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.test.InMemoryTokens;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.IndexType.FULLTEXT;

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
    private IndexPrototype nodeFtsPrototype = IndexPrototype.forSchema( fulltextNodeSchema ).withIndexType( FULLTEXT );
    private IndexPrototype relFtsPrototype = IndexPrototype.forSchema( fulltextRelSchema ).withIndexType( FULLTEXT );
    private IndexPrototype labelFtsPrototype2 = IndexPrototype.forSchema( labelSchema2 ).withIndexType( FULLTEXT );
    private IndexPrototype nodeFtsPrototype2 = IndexPrototype.forSchema( fulltextNodeSchema2 ).withIndexType( FULLTEXT );
    private IndexPrototype labelPrototypeNamed = IndexPrototype.forSchema( labelSchema ).withName( "labelPrototypeNamed" );
    private IndexPrototype labelUniquePrototypeNamed = IndexPrototype.uniqueForSchema( labelSchema ).withName( "labelUniquePrototypeNamed" );
    private IndexPrototype relTypePrototypeNamed = IndexPrototype.forSchema( relTypeSchema ).withName( "relTypePrototypeNamed" );
    private IndexPrototype relTypeUniquePrototypeNamed = IndexPrototype.uniqueForSchema( relTypeSchema ).withName( "relTypeUniquePrototypeNamed" );
    private IndexPrototype nodeFtsPrototypeNamed = IndexPrototype.forSchema( fulltextNodeSchema ).withIndexType( FULLTEXT ).withName( "nodeFtsPrototypeNamed" );
    private IndexPrototype relFtsPrototypeNamed = IndexPrototype.forSchema( fulltextRelSchema ).withIndexType( FULLTEXT ).withName( "relFtsPrototypeNamed" );
    private IndexPrototype labelFtsPrototype2Named = IndexPrototype.forSchema( labelSchema2 ).withIndexType( FULLTEXT ).withName( "labelFtsPrototype2Named" );
    private IndexPrototype nodeFtsPrototype2Named =
            IndexPrototype.forSchema( fulltextNodeSchema2 ).withIndexType( FULLTEXT ).withName( "nodeFtsPrototype2Named" );
    private IndexDescriptor labelIndexNamed = labelPrototypeNamed.withName( "labelIndexNamed" ).materialise( 1 );
    private IndexDescriptor labelUniqueIndexNamed = labelUniquePrototypeNamed.withName( "labelUniqueIndexNamed" ).materialise( 2 );
    private IndexDescriptor relTypeIndexNamed = relTypePrototypeNamed.withName( "relTypeIndexNamed" ).materialise( 3 );
    private IndexDescriptor relTypeUniqueIndexNamed = relTypeUniquePrototypeNamed.withName( "relTypeUniqueIndexNamed" ).materialise( 4 );
    private IndexDescriptor nodeFtsIndexNamed = nodeFtsPrototypeNamed.withName( "nodeFtsIndexNamed" ).materialise( 5 );
    private IndexDescriptor relFtsIndexNamed = relFtsPrototypeNamed.withName( "relFtsIndexNamed" ).materialise( 6 );
    private IndexDescriptor labelFtsIndex2Named = labelFtsPrototype2Named.withName( "labelFtsIndex2Named" ).materialise( 7 );
    private IndexDescriptor nodeFtsIndex2Named = nodeFtsPrototype2Named.withName( "nodeFtsIndex2Named" ).materialise( 8 );
    private ConstraintDescriptor uniqueLabelConstraint = ConstraintDescriptorFactory.uniqueForSchema( labelSchema );
    private ConstraintDescriptor existsLabelConstraint = ConstraintDescriptorFactory.existsForSchema( labelSchema );
    private ConstraintDescriptor nodeKeyConstraint = ConstraintDescriptorFactory.nodeKeyForSchema( labelSchema );
    private ConstraintDescriptor existsRelTypeConstraint = ConstraintDescriptorFactory.existsForSchema( relTypeSchema );
    private ConstraintDescriptor uniqueLabelConstraint2 = ConstraintDescriptorFactory.uniqueForSchema( labelSchema2 );
    private ConstraintDescriptor uniqueLabelConstraintNamed = uniqueLabelConstraint.withName( "uniqueLabelConstraintNamed" ).withId( 1 ).withOwnedIndexId( 1 );
    private ConstraintDescriptor existsLabelConstraintNamed = existsLabelConstraint.withName( "existsLabelConstraintNamed" ).withId( 2 ).withOwnedIndexId( 2 );
    private ConstraintDescriptor nodeKeyConstraintNamed = nodeKeyConstraint.withName( "nodeKeyConstraintNamed" ).withId( 3 ).withOwnedIndexId( 3 );
    private ConstraintDescriptor existsRelTypeConstraintNamed =
            existsRelTypeConstraint.withName( "existsRelTypeConstraintNamed" ).withId( 4 ).withOwnedIndexId( 4 );
    private ConstraintDescriptor uniqueLabelConstraint2Named =
            uniqueLabelConstraint2.withName( "uniqueLabelConstraint2Named" ).withId( 5 ).withOwnedIndexId( 5 );
    private InMemoryTokens lookup = new InMemoryTokens()
            .label( 0, "La:bel" ).label( 1, "Label1" ).label( 2, "Label2" )
            .relationshipType( 0, "Ty:pe" ).relationshipType( 1, "Type1" ).relationshipType( 2, "Type2" )
            .propertyKey( 0, "prop:erty" ).propertyKey( 1, "prop1" ).propertyKey( 2, "prop2" ).propertyKey( 3, "prop3" );

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
        assertUserDescription( "Index( type='GENERAL BTREE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )", labelPrototype );
        assertUserDescription( "Index( type='UNIQUE BTREE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )", labelUniquePrototype );
        assertUserDescription( "Index( type='GENERAL BTREE', schema=-[:Type1 {prop2, prop3}]-, indexProvider='Undecided-0' )", relTypePrototype );
        assertUserDescription( "Index( type='UNIQUE BTREE', schema=-[:Type1 {prop2, prop3}]-, indexProvider='Undecided-0' )", relTypeUniquePrototype );
        assertUserDescription( "Index( type='GENERAL FULLTEXT', schema=(:Label1:Label2 {prop1, prop2}), indexProvider='Undecided-0' )", nodeFtsPrototype );
        assertUserDescription( "Index( type='GENERAL FULLTEXT', schema=-[:Type1:Type2 {prop1, prop2}]-, indexProvider='Undecided-0' )", relFtsPrototype );
        assertUserDescription( "Constraint( type='UNIQUENESS', schema=(:Label1 {prop2, prop3}) )", uniqueLabelConstraint );
        assertUserDescription( "Constraint( type='NODE PROPERTY EXISTENCE', schema=(:Label1 {prop2, prop3}) )", existsLabelConstraint );
        assertUserDescription( "Constraint( type='NODE KEY', schema=(:Label1 {prop2, prop3}) )", nodeKeyConstraint );
        assertUserDescription( "Constraint( type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:Type1 {prop2, prop3}]- )", existsRelTypeConstraint );
        assertUserDescription( "Index( type='GENERAL FULLTEXT', schema=(:`La:bel` {`prop:erty`, prop1}), indexProvider='Undecided-0' )", labelFtsPrototype2 );
        assertUserDescription( "Index( type='GENERAL FULLTEXT', schema=(:`La:bel`:Label1 {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                nodeFtsPrototype2 );
        assertUserDescription( "Constraint( type='UNIQUENESS', schema=(:`La:bel` {`prop:erty`, prop1}) )", uniqueLabelConstraint2 );

        assertUserDescription( "Index( name='labelPrototypeNamed', type='GENERAL BTREE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                labelPrototypeNamed );
        assertUserDescription( "Index( name='labelUniquePrototypeNamed', type='UNIQUE BTREE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                labelUniquePrototypeNamed );
        assertUserDescription( "Index( name='relTypePrototypeNamed', type='GENERAL BTREE', schema=-[:Type1 {prop2, prop3}]-, indexProvider='Undecided-0' )",
                relTypePrototypeNamed );
        assertUserDescription(
                "Index( name='relTypeUniquePrototypeNamed', type='UNIQUE BTREE', schema=-[:Type1 {prop2, prop3}]-, indexProvider='Undecided-0' )",
                relTypeUniquePrototypeNamed );
        assertUserDescription(
                "Index( name='nodeFtsPrototypeNamed', type='GENERAL FULLTEXT', schema=(:Label1:Label2 {prop1, prop2}), indexProvider='Undecided-0' )",
                nodeFtsPrototypeNamed );
        assertUserDescription(
                "Index( name='relFtsPrototypeNamed', type='GENERAL FULLTEXT', schema=-[:Type1:Type2 {prop1, prop2}]-, indexProvider='Undecided-0' )",
                relFtsPrototypeNamed );
        assertUserDescription(
                "Index( name='labelFtsPrototype2Named', type='GENERAL FULLTEXT', schema=(:`La:bel` {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                labelFtsPrototype2Named );
        assertUserDescription(
                "Index( name='nodeFtsPrototype2Named', type='GENERAL FULLTEXT', schema=(:`La:bel`:Label1 {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                nodeFtsPrototype2Named );

        assertUserDescription( "Index( id=1, name='labelIndexNamed', type='GENERAL BTREE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                labelIndexNamed );
        assertUserDescription( "Index( id=2, name='labelUniqueIndexNamed', type='UNIQUE BTREE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                labelUniqueIndexNamed );
        assertUserDescription( "Index( id=3, name='relTypeIndexNamed', type='GENERAL BTREE', schema=-[:Type1 {prop2, prop3}]-, indexProvider='Undecided-0' )",
                relTypeIndexNamed );
        assertUserDescription(
                "Index( id=4, name='relTypeUniqueIndexNamed', type='UNIQUE BTREE', schema=-[:Type1 {prop2, prop3}]-, indexProvider='Undecided-0' )",
                relTypeUniqueIndexNamed );
        assertUserDescription(
                "Index( id=5, name='nodeFtsIndexNamed', type='GENERAL FULLTEXT', schema=(:Label1:Label2 {prop1, prop2}), indexProvider='Undecided-0' )",
                nodeFtsIndexNamed );
        assertUserDescription(
                "Index( id=6, name='relFtsIndexNamed', type='GENERAL FULLTEXT', schema=-[:Type1:Type2 {prop1, prop2}]-, indexProvider='Undecided-0' )",
                relFtsIndexNamed );
        assertUserDescription(
                "Index( id=7, name='labelFtsIndex2Named', type='GENERAL FULLTEXT', schema=(:`La:bel` {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                labelFtsIndex2Named );
        assertUserDescription(
                "Index( id=8, name='nodeFtsIndex2Named', type='GENERAL FULLTEXT', schema=(:`La:bel`:Label1 {`prop:erty`, prop1}), " +
                        "indexProvider='Undecided-0' )", nodeFtsIndex2Named );

        assertUserDescription( "Constraint( id=1, name='uniqueLabelConstraintNamed', type='UNIQUENESS', schema=(:Label1 {prop2, prop3}), ownedIndex=1 )",
                uniqueLabelConstraintNamed );
        assertUserDescription(
                "Constraint( id=2, name='existsLabelConstraintNamed', type='NODE PROPERTY EXISTENCE', schema=(:Label1 {prop2, prop3}), ownedIndex=2 )",
                existsLabelConstraintNamed );
        assertUserDescription( "Constraint( id=3, name='nodeKeyConstraintNamed', type='NODE KEY', schema=(:Label1 {prop2, prop3}), ownedIndex=3 )",
                nodeKeyConstraintNamed );
        assertUserDescription(
                "Constraint( id=4, name='existsRelTypeConstraintNamed', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:Type1 {prop2, prop3}]-, " +
                        "ownedIndex=4 )",
                existsRelTypeConstraintNamed );
        assertUserDescription(
                "Constraint( id=5, name='uniqueLabelConstraint2Named', type='UNIQUENESS', schema=(:`La:bel` {`prop:erty`, prop1}), ownedIndex=5 )",
                uniqueLabelConstraint2Named );
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
        List<String> invalidNames = List.of( "", "\0", " ", "  ", "\t", " \t ", "\n", "\r" );

        for ( String invalidName : invalidNames )
        {
            assertThrows( IllegalArgumentException.class, () -> SchemaRule.sanitiseName( invalidName ), "invalid name: '" + invalidName + "'" );
        }
    }

    @Test
    void sanitiseNameMustAcceptValidNames()
    {
        List<String> validNames = List.of(
                ".", ",", "'", "a", " a", "a ", "a b", "a\n", "a\nb", "\"", "@", "#", "$", "%", "{", "}", "\uD83D\uDE02", ":", ";", "[", "]", "-", "_", "`",
                "``", "`a`", "a`b", "a``b"  );

        for ( String validName : validNames )
        {
            SchemaRule.sanitiseName( validName );
        }
    }
}
