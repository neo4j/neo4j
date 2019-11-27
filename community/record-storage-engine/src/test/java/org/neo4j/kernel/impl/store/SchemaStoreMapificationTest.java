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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.recordstorage.RandomSchema;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.IndexType.FULLTEXT;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForSchema;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.nodeKeyForSchema;

class SchemaStoreMapificationTest
{
    private static final RandomSchema RANDOM_SCHEMA = new RandomSchema();

    private LabelSchemaDescriptor labelSchema = SchemaDescriptor.forLabel( 1, 2, 3 );
    private RelationTypeSchemaDescriptor relTypeSchema = SchemaDescriptor.forRelType( 1, 2, 3 );
    private FulltextSchemaDescriptor fulltextNodeSchema = SchemaDescriptor.fulltext( NODE, new int[]{1, 2}, new int[]{1, 2} );
    private FulltextSchemaDescriptor fulltextRelSchema = SchemaDescriptor.fulltext( RELATIONSHIP, new int[]{1, 2}, new int[]{1, 2} );
    private IndexProviderDescriptor tree = new IndexProviderDescriptor( NATIVE_BTREE10.providerKey(), NATIVE_BTREE10.providerVersion() );
    private IndexProviderDescriptor fts = new IndexProviderDescriptor( "fulltext", "1.0" );
    private IndexDescriptor labelIndex = forSchema( labelSchema, tree ).withName( "labelIndex" ).materialise( 1 );
    private IndexDescriptor labelUniqueIndex = uniqueForSchema( labelSchema, tree ).withName( "labelUniqueIndex" ).materialise( 1 );
    private IndexDescriptor relTypeIndex = forSchema( relTypeSchema, tree ).withName( "relTypeIndex" ).materialise( 1 );
    private IndexDescriptor relTypeUniqueIndex = uniqueForSchema( relTypeSchema, tree ).withName( "relTypeUniqueIndex" ).materialise( 1 );
    private IndexDescriptor nodeFtsIndex = forSchema( fulltextNodeSchema, fts ).withIndexType( FULLTEXT ).withName( "nodeFtsIndex" ).materialise( 1 );
    private IndexDescriptor relFtsIndex = forSchema( fulltextRelSchema, fts ).withIndexType( FULLTEXT ).withName( "relFtsIndex" ).materialise( 1 );
    private ConstraintDescriptor uniqueLabelConstraint =
            ConstraintDescriptorFactory.uniqueForSchema( labelSchema ).withName( "uniqueLabelConstraint" ).withId( 1 );
    private ConstraintDescriptor existsLabelConstraint = existsForSchema( labelSchema ).withName( "existsLabelConstraint" ).withId( 1 );
    private ConstraintDescriptor nodeKeyConstraint = nodeKeyForSchema( labelSchema ).withName( "nodeKeyConstraint" ).withId( 1 );
    private ConstraintDescriptor existsRelTypeConstraint = existsForSchema( relTypeSchema ).withName( "existsRelTypeConstraint" ).withId( 1 );

    @RepeatedTest( 500 )
    void mapificationMustPreserveSchemaRulesAccurately() throws MalformedSchemaRuleException
    {
        SchemaRule rule = RANDOM_SCHEMA.get();
        Map<String,Value> mapified = SchemaStore.mapifySchemaRule( rule );
        SchemaRule unmapified = SchemaStore.unmapifySchemaRule( rule.getId(), mapified );
        if ( !rule.equals( unmapified ) || !unmapified.equals( rule ) )
        {
            fail( "Mapification of schema rule was not fully reversible.\n" +
                    "Expected: " + rule + "\n" +
                    "But got:  " + unmapified + "\n" +
                    "Mapified rule: " + mapified );
        }
    }

    @Test
    void labelIndexDeterministicUnmapification() throws Exception
    {
        // Index( 1, 'labelIndex', GENERAL BTREE, :label[1](property[2], property[3]), native-btree-1.0 )
        Map<String,Value> mapified = Map.of(
                "__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue( "NODE" ),
                "__org.neo4j.SchemaRule.name", Values.stringValue( "labelIndex" ),
                "__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue( "COMPLETE_ALL_TOKENS" ),
                "__org.neo4j.SchemaRule.indexProviderName", Values.stringValue( "native-btree" ),
                "__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue( "1.0" ),
                "__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray( new int[] {2,3} ),
                "__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue( "INDEX" ),
                "__org.neo4j.SchemaRule.indexType", Values.stringValue( "BTREE" ),
                "__org.neo4j.SchemaRule.indexRuleType", Values.stringValue( "NON_UNIQUE" ),
                "__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray( new int[] {1} )
        );
        assertThat( SchemaStore.unmapifySchemaRule( 1, mapified ) ).isEqualTo( labelIndex );
    }

    @Test
    void labelUniqueIndexDeterministicUnmapification() throws Exception
    {
        // Index( 1, 'labelUniqueIndex', UNIQUE BTREE, :label[1](property[2], property[3]), native-btree-1.0 )
        Map<String,Value> mapified = Map.of(
                "__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue( "NODE" ),
                "__org.neo4j.SchemaRule.name", Values.stringValue( "labelUniqueIndex" ),
                "__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue( "COMPLETE_ALL_TOKENS" ),
                "__org.neo4j.SchemaRule.indexProviderName", Values.stringValue( "native-btree" ),
                "__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue( "1.0" ),
                "__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray( new int[] {2,3} ),
                "__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue( "INDEX" ),
                "__org.neo4j.SchemaRule.indexType", Values.stringValue( "BTREE" ),
                "__org.neo4j.SchemaRule.indexRuleType", Values.stringValue( "UNIQUE" ),
                "__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray( new int[] {1} )
        );
        assertThat( SchemaStore.unmapifySchemaRule( 1, mapified ) ).isEqualTo( labelUniqueIndex );
    }

    @Test
    void relTypeIndexDeterministicUnmapification() throws Exception
    {
        // Index( 1, 'relTypeIndex', GENERAL BTREE, -[:relType[1](property[2], property[3])]-, native-btree-1.0 )
        Map<String,Value> mapified = Map.of(
                "__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue( "RELATIONSHIP" ),
                "__org.neo4j.SchemaRule.name", Values.stringValue( "relTypeIndex" ),
                "__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue( "COMPLETE_ALL_TOKENS" ),
                "__org.neo4j.SchemaRule.indexProviderName", Values.stringValue( "native-btree" ),
                "__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue( "1.0" ),
                "__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray( new int[] {2,3} ),
                "__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue( "INDEX" ),
                "__org.neo4j.SchemaRule.indexType", Values.stringValue( "BTREE" ),
                "__org.neo4j.SchemaRule.indexRuleType", Values.stringValue( "NON_UNIQUE" ),
                "__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray( new int[] {1} )
        );
        assertThat( SchemaStore.unmapifySchemaRule( 1, mapified ) ).isEqualTo( relTypeIndex );
    }

    @Test
    void relTypeUniqueIndexDeterministicUnmapification() throws Exception
    {
        // Index( 1, 'relTypeUniqueIndex', UNIQUE BTREE, -[:relType[1](property[2], property[3])]-, native-btree-1.0 )
        Map<String,Value> mapified = Map.of(
                "__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue( "RELATIONSHIP" ),
                "__org.neo4j.SchemaRule.name", Values.stringValue( "relTypeUniqueIndex" ),
                "__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue( "COMPLETE_ALL_TOKENS" ),
                "__org.neo4j.SchemaRule.indexProviderName", Values.stringValue( "native-btree" ),
                "__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue( "1.0" ),
                "__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray( new int[] {2,3} ),
                "__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue( "INDEX" ),
                "__org.neo4j.SchemaRule.indexType", Values.stringValue( "BTREE" ),
                "__org.neo4j.SchemaRule.indexRuleType", Values.stringValue( "UNIQUE" ),
                "__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray( new int[] {1} )
        );
        assertThat( SchemaStore.unmapifySchemaRule( 1, mapified ) ).isEqualTo( relTypeUniqueIndex );
    }

    @Test
    void nodeFtsIndexDeterministicUnmapification() throws Exception
    {
        // Index( 1, 'nodeFtsIndex', GENERAL FULLTEXT, :label[1],label[2](property[1], property[2]), fulltext-1.0 )
        Map<String,Value> mapified = Map.of(
                "__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue( "NODE" ),
                "__org.neo4j.SchemaRule.name", Values.stringValue( "nodeFtsIndex" ),
                "__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue( "PARTIAL_ANY_TOKEN" ),
                "__org.neo4j.SchemaRule.indexProviderName", Values.stringValue( "fulltext" ),
                "__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue( "1.0" ),
                "__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray( new int[] {1,2} ),
                "__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue( "INDEX" ),
                "__org.neo4j.SchemaRule.indexType", Values.stringValue( "FULLTEXT" ),
                "__org.neo4j.SchemaRule.indexRuleType", Values.stringValue( "NON_UNIQUE" ),
                "__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray( new int[] {1,2} )
        );
        assertThat( SchemaStore.unmapifySchemaRule( 1, mapified ) ).isEqualTo( nodeFtsIndex );
    }

    @Test
    void relFtsIndexDeterministicUnmapification() throws Exception
    {
        // Index( 1, 'relFtsIndex', GENERAL FULLTEXT, -[:relType[1],relType[2](property[1], property[2])]-, fulltext-1.0 )
        Map<String,Value> mapified = Map.of(
                "__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue( "RELATIONSHIP" ),
                "__org.neo4j.SchemaRule.name", Values.stringValue( "relFtsIndex" ),
                "__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue( "PARTIAL_ANY_TOKEN" ),
                "__org.neo4j.SchemaRule.indexProviderName", Values.stringValue( "fulltext" ),
                "__org.neo4j.SchemaRule.indexProviderVersion", Values.stringValue( "1.0" ),
                "__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray( new int[] {1,2} ),
                "__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue( "INDEX" ),
                "__org.neo4j.SchemaRule.indexType", Values.stringValue( "FULLTEXT" ),
                "__org.neo4j.SchemaRule.indexRuleType", Values.stringValue( "NON_UNIQUE" ),
                "__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray( new int[] {1,2} )
        );
        assertThat( SchemaStore.unmapifySchemaRule( 1, mapified ) ).isEqualTo( relFtsIndex );
    }

    @Test
    void uniqueLabelConstraintDeterministicUnmapification() throws Exception
    {
        // org.neo4j.internal.schema.constraints.ConstraintDescriptorImplementation@402007
        Map<String,Value> mapified = Map.of(
                "__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue( "NODE" ),
                "__org.neo4j.SchemaRule.name", Values.stringValue( "uniqueLabelConstraint" ),
                "__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue( "COMPLETE_ALL_TOKENS" ),
                "__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray( new int[] {2,3} ),
                "__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue( "CONSTRAINT" ),
                "__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue( "UNIQUE" ),
                "__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray( new int[] {1} )
        );
        assertThat( SchemaStore.unmapifySchemaRule( 1, mapified ) ).isEqualTo( uniqueLabelConstraint );
    }

    @Test
    void existsLabelConstraintDeterministicUnmapification() throws Exception
    {
        // org.neo4j.internal.schema.constraints.ConstraintDescriptorImplementation@41402017
        Map<String,Value> mapified = Map.of(
                "__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue( "NODE" ),
                "__org.neo4j.SchemaRule.name", Values.stringValue( "existsLabelConstraint" ),
                "__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue( "COMPLETE_ALL_TOKENS" ),
                "__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray( new int[] {2,3} ),
                "__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue( "CONSTRAINT" ),
                "__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue( "EXISTS" ),
                "__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray( new int[] {1} )
        );
        assertThat( SchemaStore.unmapifySchemaRule( 1, mapified ) ).isEqualTo( existsLabelConstraint );
    }

    @Test
    void nodeKeyConstraintDeterministicUnmapification() throws Exception
    {
        // org.neo4j.internal.schema.constraints.ConstraintDescriptorImplementation@40142211
        Map<String,Value> mapified = Map.of(
                "__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue( "NODE" ),
                "__org.neo4j.SchemaRule.name", Values.stringValue( "nodeKeyConstraint" ),
                "__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue( "COMPLETE_ALL_TOKENS" ),
                "__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray( new int[] {2,3} ),
                "__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue( "CONSTRAINT" ),
                "__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue( "UNIQUE_EXISTS" ),
                "__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray( new int[] {1} )
        );
        assertThat( SchemaStore.unmapifySchemaRule( 1, mapified ) ).isEqualTo( nodeKeyConstraint );
    }

    @Test
    void existsRelTypeConstraintDeterministicUnmapification() throws Exception
    {
        // org.neo4j.internal.schema.constraints.ConstraintDescriptorImplementation@40083801
        Map<String,Value> mapified = Map.of(
                "__org.neo4j.SchemaRule.schemaEntityType", Values.stringValue( "RELATIONSHIP" ),
                "__org.neo4j.SchemaRule.name", Values.stringValue( "existsRelTypeConstraint" ),
                "__org.neo4j.SchemaRule.schemaPropertySchemaType", Values.stringValue( "COMPLETE_ALL_TOKENS" ),
                "__org.neo4j.SchemaRule.schemaPropertyIds", Values.intArray( new int[] {2,3} ),
                "__org.neo4j.SchemaRule.schemaRuleType", Values.stringValue( "CONSTRAINT" ),
                "__org.neo4j.SchemaRule.constraintRuleType", Values.stringValue( "EXISTS" ),
                "__org.neo4j.SchemaRule.schemaEntityIds", Values.intArray( new int[] {1} )
        );
        assertThat( SchemaStore.unmapifySchemaRule( 1, mapified ) ).isEqualTo( existsRelTypeConstraint );
    }
}
