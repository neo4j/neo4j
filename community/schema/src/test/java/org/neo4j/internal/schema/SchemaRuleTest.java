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
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.internal.schema.IndexType.POINT;
import static org.neo4j.internal.schema.IndexType.RANGE;
import static org.neo4j.internal.schema.IndexType.TEXT;

class SchemaRuleTest
{
    private final LabelSchemaDescriptor labelSchema = SchemaDescriptors.forLabel( 1, 2, 3 );
    private final LabelSchemaDescriptor labelSchema2 = SchemaDescriptors.forLabel( 0, 0, 1 );
    private final RelationTypeSchemaDescriptor relTypeSchema = SchemaDescriptors.forRelType( 1, 2, 3 );
    private final FulltextSchemaDescriptor fulltextNodeSchema = SchemaDescriptors.fulltext( NODE, new int[]{1, 2}, new int[]{1, 2} );
    private final FulltextSchemaDescriptor fulltextRelSchema = SchemaDescriptors.fulltext( RELATIONSHIP, new int[]{1, 2}, new int[]{1, 2} );
    private final FulltextSchemaDescriptor fulltextNodeSchema2 = SchemaDescriptors.fulltext( NODE, new int[]{0, 1}, new int[]{0, 1} );
    private final AnyTokenSchemaDescriptor allLabelsSchema = SchemaDescriptors.forAnyEntityTokens( NODE );
    private final AnyTokenSchemaDescriptor allRelTypesSchema = SchemaDescriptors.forAnyEntityTokens( RELATIONSHIP );
    private final LabelSchemaDescriptor labelSinglePropSchema = SchemaDescriptors.forLabel( 1, 2 );
    private final RelationTypeSchemaDescriptor relTypeSinglePropSchema = SchemaDescriptors.forRelType( 1, 2 );
    private final IndexPrototype rangeLabelPrototype = IndexPrototype.forSchema( labelSchema ).withIndexType( RANGE );
    private final IndexPrototype rangeLabelPrototype2 = IndexPrototype.forSchema( labelSchema2 ).withIndexType( RANGE );
    private final IndexPrototype rangeLabelUniquePrototype = IndexPrototype.uniqueForSchema( labelSchema ).withIndexType( RANGE );
    private final IndexPrototype rangeRelTypePrototype = IndexPrototype.forSchema( relTypeSchema ).withIndexType( RANGE );
    private final IndexPrototype rangeRelTypeUniquePrototype = IndexPrototype.uniqueForSchema( relTypeSchema ).withIndexType( RANGE );
    private final IndexPrototype nodeFtsPrototype = IndexPrototype.forSchema( fulltextNodeSchema ).withIndexType( FULLTEXT );
    private final IndexPrototype relFtsPrototype = IndexPrototype.forSchema( fulltextRelSchema ).withIndexType( FULLTEXT );
    private final IndexPrototype nodeFtsPrototype2 = IndexPrototype.forSchema( fulltextNodeSchema2 ).withIndexType( FULLTEXT );
    private final IndexPrototype allLabelsPrototype = IndexPrototype.forSchema( allLabelsSchema ).withIndexType( LOOKUP );
    private final IndexPrototype allRelTypesPrototype = IndexPrototype.forSchema( allRelTypesSchema ).withIndexType( LOOKUP );
    private final IndexPrototype textLabelPrototype = IndexPrototype.forSchema( labelSinglePropSchema ).withIndexType( TEXT );
    private final IndexPrototype textRelTypePrototype = IndexPrototype.forSchema( relTypeSinglePropSchema ).withIndexType( TEXT );
    private final IndexPrototype pointLabelPrototype = IndexPrototype.forSchema( labelSinglePropSchema ).withIndexType( POINT );
    private final IndexPrototype pointRelTypePrototype = IndexPrototype.forSchema( relTypeSinglePropSchema ).withIndexType( POINT );
    private final IndexPrototype rangeLabelPrototypeNamed = rangeLabelPrototype.withName( "rangeLabelPrototypeNamed" );
    private final IndexPrototype rangeLabelPrototype2Named = IndexPrototype.forSchema( labelSchema2 ).withName( "labelPrototype2Named" );
    private final IndexPrototype rangeLabelUniquePrototypeNamed = rangeLabelUniquePrototype.withName( "rangeLabelUniquePrototypeNamed" );
    private final IndexPrototype rangeRelTypePrototypeNamed = rangeRelTypePrototype.withName( "rangeRelTypePrototypeNamed" );
    private final IndexPrototype rangeRelTypeUniquePrototypeNamed = rangeRelTypeUniquePrototype.withName( "rangeRelTypeUniquePrototypeNamed" );
    private final IndexPrototype nodeFtsPrototypeNamed =
            IndexPrototype.forSchema( fulltextNodeSchema ).withIndexType( FULLTEXT ).withName( "nodeFtsPrototypeNamed" );
    private final IndexPrototype relFtsPrototypeNamed =
            IndexPrototype.forSchema( fulltextRelSchema ).withIndexType( FULLTEXT ).withName( "relFtsPrototypeNamed" );
    private final IndexPrototype nodeFtsPrototype2Named =
            IndexPrototype.forSchema( fulltextNodeSchema2 ).withIndexType( FULLTEXT ).withName( "nodeFtsPrototype2Named" );
    private final IndexPrototype allLabelsPrototypeNamed =
            IndexPrototype.forSchema( allLabelsSchema ).withIndexType( LOOKUP ).withName( "allLabelsPrototypeNamed" );
    private final IndexPrototype allRelTypesPrototypeNamed =
            IndexPrototype.forSchema( allRelTypesSchema ).withIndexType( LOOKUP ).withName( "allRelTypesPrototypeNamed" );
    private final IndexPrototype textLabelPrototypeNamed = textLabelPrototype.withName( "textLabelPrototypeNamed" );
    private final IndexPrototype textRelTypePrototypeNamed = textRelTypePrototype.withName( "textRelTypePrototypeNamed" );
    private final IndexPrototype pointLabelPrototypeNamed = pointLabelPrototype.withName( "pointLabelPrototypeNamed" );
    private final IndexPrototype pointRelTypePrototypeNamed = pointRelTypePrototype.withName( "pointRelTypePrototypeNamed" );
    private final IndexDescriptor rangeLabelIndexNamed = rangeLabelPrototypeNamed.withName( "rangeLabelIndexNamed" ).materialise( 1 );
    private final IndexDescriptor rangeLabelIndex2Named = rangeLabelPrototype2Named.withName( "labelIndex2Named" ).materialise(  2 );
    private final IndexDescriptor rangeLabelUniqueIndexNamed = rangeLabelUniquePrototypeNamed.withName( "rangeLabelUniqueIndexNamed" ).materialise( 3 );
    private final IndexDescriptor rangeRelTypeIndexNamed = rangeRelTypePrototypeNamed.withName( "rangeRelTypeIndexNamed" ).materialise( 4 );
    private final IndexDescriptor rangeRelTypeUniqueIndexNamed = rangeRelTypeUniquePrototypeNamed.withName( "rangeRelTypeUniqueIndexNamed" ).materialise( 5 );
    private final IndexDescriptor nodeFtsIndexNamed = nodeFtsPrototypeNamed.withName( "nodeFtsIndexNamed" ).materialise( 6 );
    private final IndexDescriptor relFtsIndexNamed = relFtsPrototypeNamed.withName( "relFtsIndexNamed" ).materialise( 7 );
    private final IndexDescriptor nodeFtsIndex2Named = nodeFtsPrototype2Named.withName( "nodeFtsIndex2Named" ).materialise( 8 );
    private final IndexDescriptor allLabelsIndexNamed = allLabelsPrototypeNamed.withName( "allLabelsIndexNamed" ).materialise( 9 );
    private final IndexDescriptor allRelTypesIndexNamed = allRelTypesPrototypeNamed.withName( "allRelTypesIndexNamed" ).materialise( 10 );
    private final IndexDescriptor textLabelIndexNamed = textLabelPrototypeNamed.withName( "textLabelIndexNamed" ).materialise( 11 );
    private final IndexDescriptor textRelTypeIndexNamed = textRelTypePrototypeNamed.withName( "textRelTypeIndexNamed" ).materialise( 12 );
    private final IndexDescriptor pointLabelIndexNamed = pointLabelPrototypeNamed.withName( "pointLabelIndexNamed" ).materialise( 13 );
    private final IndexDescriptor pointRelTypeIndexNamed = pointRelTypePrototypeNamed.withName( "pointRelTypeIndexNamed" ).materialise( 14 );
    private final IndexDescriptor indexBelongingToConstraint =
            rangeLabelUniquePrototypeNamed.withName( "indexBelongingToConstraint" ).materialise( 15 ).withOwningConstraintId( 1 );
    private final ConstraintDescriptor uniqueLabelConstraint = ConstraintDescriptorFactory.uniqueForSchema( labelSchema, RANGE );
    private final ConstraintDescriptor existsLabelConstraint = ConstraintDescriptorFactory.existsForSchema( labelSchema );
    private final ConstraintDescriptor nodeKeyConstraint = ConstraintDescriptorFactory.nodeKeyForSchema( labelSchema, RANGE );
    private final ConstraintDescriptor existsRelTypeConstraint = ConstraintDescriptorFactory.existsForSchema( relTypeSchema );
    private final ConstraintDescriptor uniqueLabelConstraint2 = ConstraintDescriptorFactory.uniqueForSchema( labelSchema2 );
    private final ConstraintDescriptor uniqueLabelConstraintNamed =
            uniqueLabelConstraint.withName( "uniqueLabelConstraintNamed" ).withId( 1 ).withOwnedIndexId( 1 );
    private final ConstraintDescriptor existsLabelConstraintNamed =
            existsLabelConstraint.withName( "existsLabelConstraintNamed" ).withId( 2 );
    private final ConstraintDescriptor nodeKeyConstraintNamed =
            nodeKeyConstraint.withName( "nodeKeyConstraintNamed" ).withId( 3 ).withOwnedIndexId( 3 );
    private final ConstraintDescriptor existsRelTypeConstraintNamed =
            existsRelTypeConstraint.withName( "existsRelTypeConstraintNamed" ).withId( 4 );
    private final ConstraintDescriptor uniqueLabelConstraint2Named =
            uniqueLabelConstraint2.withName( "uniqueLabelConstraint2Named" ).withId( 5 ).withOwnedIndexId( 5 );
    private final InMemoryTokens lookup = new InMemoryTokens()
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
        assertName( rangeLabelPrototype, "index_c4551613" );
        assertName( rangeLabelUniquePrototype, "index_ad093035" );
        assertName( rangeRelTypePrototype, "index_31eed16b" );
        assertName( rangeRelTypeUniquePrototype, "index_9b4ce430" );
        assertName( nodeFtsPrototype, "index_99c88876" );
        assertName( relFtsPrototype, "index_9c14864e" );
        assertName( uniqueLabelConstraint, "constraint_dbf17751" );
        assertName( existsLabelConstraint, "constraint_b23c1483" );
        assertName( nodeKeyConstraint, "constraint_75ad9cd9" );
        assertName( existsRelTypeConstraint, "constraint_ef4bbcac" );
        assertName( allLabelsPrototype, "index_f56fb29d" );
        assertName( allRelTypesPrototype, "index_9625776f" );
        assertName( textLabelPrototype, "index_e76ccd25" );
        assertName( textRelTypePrototype, "index_52ad048c" );
        assertName( pointLabelPrototype, "index_abc433e9" );
        assertName( pointRelTypePrototype, "index_97015bc0" );
    }

    @Test
    void mustGenerateReasonableUserDescription()
    {

        assertUserDescription( "Index( type='GENERAL RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )", rangeLabelPrototype );
        assertUserDescription( "Index( type='GENERAL RANGE', schema=(:`La:bel` {`prop:erty`, prop1}), indexProvider='Undecided-0' )", rangeLabelPrototype2 );
        assertUserDescription( "Index( type='UNIQUE RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )", rangeLabelUniquePrototype );
        assertUserDescription( "Index( type='GENERAL RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )", rangeRelTypePrototype );
        assertUserDescription( "Index( type='UNIQUE RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )", rangeRelTypeUniquePrototype );
        assertUserDescription( "Index( type='GENERAL FULLTEXT', schema=(:Label1:Label2 {prop1, prop2}), indexProvider='Undecided-0' )", nodeFtsPrototype );
        assertUserDescription( "Index( type='GENERAL FULLTEXT', schema=()-[:Type1:Type2 {prop1, prop2}]-(), indexProvider='Undecided-0' )", relFtsPrototype );
        assertUserDescription( "Index( type='TOKEN LOOKUP', schema=(:<any-labels>), indexProvider='Undecided-0' )", allLabelsPrototype );
        assertUserDescription( "Index( type='TOKEN LOOKUP', schema=()-[:<any-types>]-(), indexProvider='Undecided-0' )", allRelTypesPrototype );
        assertUserDescription( "Index( type='GENERAL TEXT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )", textLabelPrototype );
        assertUserDescription( "Index( type='GENERAL TEXT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )", textRelTypePrototype );
        assertUserDescription( "Index( type='GENERAL POINT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )", pointLabelPrototype );
        assertUserDescription( "Index( type='GENERAL POINT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )", pointRelTypePrototype );
        assertUserDescription( "Constraint( type='NODE PROPERTY EXISTENCE', schema=(:Label1 {prop2, prop3}) )", existsLabelConstraint );
        assertUserDescription( "Constraint( type='RELATIONSHIP PROPERTY EXISTENCE', schema=()-[:Type1 {prop2, prop3}]-() )", existsRelTypeConstraint );
        assertUserDescription( "Index( type='GENERAL FULLTEXT', schema=(:`La:bel`:Label1 {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                nodeFtsPrototype2 );
        assertUserDescription( "Constraint( type='UNIQUENESS', schema=(:`La:bel` {`prop:erty`, prop1}) )", uniqueLabelConstraint2 );

        assertUserDescription( "Index( name='rangeLabelPrototypeNamed', type='GENERAL RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                rangeLabelPrototypeNamed );
        assertUserDescription(
                "Index( name='labelPrototype2Named', type='GENERAL RANGE', schema=(:`La:bel` {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                rangeLabelPrototype2Named );
        assertUserDescription(
                "Index( name='rangeLabelUniquePrototypeNamed', type='UNIQUE RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                rangeLabelUniquePrototypeNamed );
        assertUserDescription(
                "Index( name='rangeRelTypePrototypeNamed', type='GENERAL RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )",
                rangeRelTypePrototypeNamed );
        assertUserDescription(
                "Index( name='rangeRelTypeUniquePrototypeNamed', type='UNIQUE RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )",
                rangeRelTypeUniquePrototypeNamed );
        assertUserDescription(
                "Index( name='nodeFtsPrototypeNamed', type='GENERAL FULLTEXT', schema=(:Label1:Label2 {prop1, prop2}), indexProvider='Undecided-0' )",
                nodeFtsPrototypeNamed );
        assertUserDescription(
                "Index( name='relFtsPrototypeNamed', type='GENERAL FULLTEXT', schema=()-[:Type1:Type2 {prop1, prop2}]-(), indexProvider='Undecided-0' )",
                relFtsPrototypeNamed );
        assertUserDescription(
                "Index( name='nodeFtsPrototype2Named', type='GENERAL FULLTEXT', schema=(:`La:bel`:Label1 {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                nodeFtsPrototype2Named );
        assertUserDescription( "Index( name='allLabelsPrototypeNamed', type='TOKEN LOOKUP', schema=(:<any-labels>), indexProvider='Undecided-0' )",
                allLabelsPrototypeNamed );
        assertUserDescription( "Index( name='allRelTypesPrototypeNamed', type='TOKEN LOOKUP', schema=()-[:<any-types>]-(), indexProvider='Undecided-0' )",
                allRelTypesPrototypeNamed );
        assertUserDescription( "Index( name='textLabelPrototypeNamed', type='GENERAL TEXT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )",
                textLabelPrototypeNamed );
        assertUserDescription( "Index( name='textRelTypePrototypeNamed', type='GENERAL TEXT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )",
                textRelTypePrototypeNamed );
        assertUserDescription( "Index( name='pointLabelPrototypeNamed', type='GENERAL POINT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )",
                pointLabelPrototypeNamed );
        assertUserDescription( "Index( name='pointRelTypePrototypeNamed', type='GENERAL POINT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )",
                pointRelTypePrototypeNamed );

        assertUserDescription( "Index( id=1, name='rangeLabelIndexNamed', type='GENERAL RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                rangeLabelIndexNamed );
        assertUserDescription(
                "Index( id=2, name='labelIndex2Named', type='GENERAL RANGE', schema=(:`La:bel` {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                rangeLabelIndex2Named );
        assertUserDescription(
                "Index( id=3, name='rangeLabelUniqueIndexNamed', type='UNIQUE RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                rangeLabelUniqueIndexNamed );
        assertUserDescription(
                "Index( id=4, name='rangeRelTypeIndexNamed', type='GENERAL RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )",
                rangeRelTypeIndexNamed );
        assertUserDescription(
                "Index( id=5, name='rangeRelTypeUniqueIndexNamed', type='UNIQUE RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )",
                rangeRelTypeUniqueIndexNamed );
        assertUserDescription(
                "Index( id=6, name='nodeFtsIndexNamed', type='GENERAL FULLTEXT', schema=(:Label1:Label2 {prop1, prop2}), indexProvider='Undecided-0' )",
                nodeFtsIndexNamed );
        assertUserDescription(
                "Index( id=7, name='relFtsIndexNamed', type='GENERAL FULLTEXT', schema=()-[:Type1:Type2 {prop1, prop2}]-(), indexProvider='Undecided-0' )",
                relFtsIndexNamed );
        assertUserDescription(
                "Index( id=8, name='nodeFtsIndex2Named', type='GENERAL FULLTEXT', schema=(:`La:bel`:Label1 {`prop:erty`, prop1}), " +
                "indexProvider='Undecided-0' )", nodeFtsIndex2Named );
        assertUserDescription( "Index( id=9, name='allLabelsIndexNamed', type='TOKEN LOOKUP', schema=(:<any-labels>), indexProvider='Undecided-0' )",
                allLabelsIndexNamed );
        assertUserDescription( "Index( id=10, name='allRelTypesIndexNamed', type='TOKEN LOOKUP', schema=()-[:<any-types>]-(), indexProvider='Undecided-0' )",
                allRelTypesIndexNamed );
        assertUserDescription( "Index( id=11, name='textLabelIndexNamed', type='GENERAL TEXT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )",
                textLabelIndexNamed );
        assertUserDescription( "Index( id=12, name='textRelTypeIndexNamed', type='GENERAL TEXT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )",
                textRelTypeIndexNamed );
        assertUserDescription( "Index( id=13, name='pointLabelIndexNamed', type='GENERAL POINT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )",
                pointLabelIndexNamed );
        assertUserDescription(
                "Index( id=14, name='pointRelTypeIndexNamed', type='GENERAL POINT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )",
                pointRelTypeIndexNamed );
        assertUserDescription(
                "Index( id=15, name='indexBelongingToConstraint', type='UNIQUE RANGE', schema=(:Label1 {prop2, prop3}), " +
                "indexProvider='Undecided-0', owningConstraint=1 )", indexBelongingToConstraint );

        assertUserDescription( "Constraint( id=1, name='uniqueLabelConstraintNamed', type='UNIQUENESS', schema=(:Label1 {prop2, prop3}), ownedIndex=1 )",
                uniqueLabelConstraintNamed );
        assertUserDescription(
                "Constraint( id=2, name='existsLabelConstraintNamed', type='NODE PROPERTY EXISTENCE', schema=(:Label1 {prop2, prop3}) )",
                existsLabelConstraintNamed );
        assertUserDescription( "Constraint( id=3, name='nodeKeyConstraintNamed', type='NODE KEY', schema=(:Label1 {prop2, prop3}), ownedIndex=3 )",
                nodeKeyConstraintNamed );
        assertUserDescription(
                "Constraint( id=4, name='existsRelTypeConstraintNamed', type='RELATIONSHIP PROPERTY EXISTENCE', schema=()-[:Type1 {prop2, prop3}]-() )",
                existsRelTypeConstraintNamed );
        assertUserDescription(
                "Constraint( id=5, name='uniqueLabelConstraint2Named', type='UNIQUENESS', schema=(:`La:bel` {`prop:erty`, prop1}), ownedIndex=5 )",
                uniqueLabelConstraint2Named );
    }

    private static void assertName( SchemaDescriptorSupplier schemaish, String expectedName )
    {
        String generateName = SchemaNameUtil.generateName( schemaish, new String[]{"A"}, new String[]{"B", "C"} );
        assertThat( generateName ).isEqualTo( expectedName );
        assertThat( SchemaNameUtil.sanitiseName( generateName ) ).isEqualTo( expectedName );
    }

    private void assertUserDescription( String description, SchemaDescriptorSupplier schemaish )
    {
        assertEquals( description, schemaish.userDescription( lookup ), "wrong userDescription for " + schemaish );
    }

    @SuppressWarnings( {"OptionalAssignedToNull", "ConstantConditions"} )
    @Test
    void sanitiseNameMustRejectEmptyOptionalOrNullNames()
    {
        assertThrows( IllegalArgumentException.class, () -> SchemaNameUtil.sanitiseName( Optional.empty() ) );
        assertThrows( NullPointerException.class, () -> SchemaNameUtil.sanitiseName( (Optional<String>) null ) );
        assertThrows( IllegalArgumentException.class, () -> SchemaNameUtil.sanitiseName( (String) null ) );
    }

    @Test
    void sanitiseNameMustRejectReservedNames()
    {
        Set<String> reservedNames = ReservedSchemaRuleNames.getReservedNames();
        reservedNames = reservedNames.stream().flatMap( n -> Stream.of( " " + n, n, n + " " ) ).collect( Collectors.toSet() );
        for ( String reservedName : reservedNames )
        {
            assertThrows( IllegalArgumentException.class, () -> SchemaNameUtil.sanitiseName( reservedName ), "reserved name: '" + reservedName + "'" );
        }
    }

    @Test
    void sanitiseNameMustRejectInvalidNames()
    {
        List<String> invalidNames = List.of( "", "\0", " ", "  ", "\t", " \t ", "\n", "\r" );

        for ( String invalidName : invalidNames )
        {
            assertThrows( IllegalArgumentException.class, () -> SchemaNameUtil.sanitiseName( invalidName ), "invalid name: '" + invalidName + "'" );
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
            SchemaNameUtil.sanitiseName( validName );
        }
    }
}
