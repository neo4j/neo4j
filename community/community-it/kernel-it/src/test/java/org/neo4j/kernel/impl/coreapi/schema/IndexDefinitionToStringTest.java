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
package org.neo4j.kernel.impl.coreapi.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.AnyTokens;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings.enable_relationship_property_indexes;
import static org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
class IndexDefinitionToStringTest
{
    @Inject
    private GraphDatabaseService db;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( enable_scan_stores_as_token_indexes, true );
        builder.setConfig( enable_relationship_property_indexes, true );
    }

    @BeforeEach
    void setup()
    {
        try ( var tx = db.beginTx() )
        {
            tx.schema().getIndexes().forEach( IndexDefinition::drop );
            tx.commit();
        }
    }

    @Test
    void testToString()
    {
        try ( var tx = db.beginTx() )
        {
            var labelTokenIndex = tx.schema().indexFor( AnyTokens.ANY_LABELS ).withName( "labelTokenIndex" ).create();
            var labelProperty = tx.schema().indexFor( Label.label( "someLabel" ) )
                                  .on( "someProperty" ).withName( "labelIndexName" ).create();
            var labelProperties = tx.schema().indexFor( Label.label( "someLabel" ) )
                                    .on( "someProperty" ).on( "someOtherProperty" )
                                    .withName( "labelIndexNames" ).create();

            var relTypeTokenIndex = tx.schema().indexFor( AnyTokens.ANY_RELATIONSHIP_TYPES ).withName( "relTypeTokenIndex" ).create();
            var relTypeProperty = tx.schema().indexFor( RelationshipType.withName( "someRelationship" ) )
                                    .on( "someProperty" ).withName( "relTypeIndexName" ).create();
            var relTypeProperties = tx.schema().indexFor( RelationshipType.withName( "someRelationship" ) )
                                      .on( "someProperty" ).on( "someOtherProperty" )
                                      .withName( "relTypeIndexNames" ).create();

            assertIndexString( labelTokenIndex,
                               "IndexDefinition[label:<any-labels>] " +
                               "(Index( id=%d, name='labelTokenIndex', type='GENERAL LOOKUP', " +
                               "schema=(:<any-labels>), indexProvider='token-1.0' ))" );
            assertIndexString( labelProperty,
                               "IndexDefinition[label:someLabel on:someProperty] " +
                               "(Index( id=%d, name='labelIndexName', type='GENERAL BTREE', " +
                               "schema=(:someLabel {someProperty}), indexProvider='native-btree-1.0' ))" );
            assertIndexString( labelProperties,
                               "IndexDefinition[label:someLabel on:someProperty,someOtherProperty] " +
                               "(Index( id=%d, name='labelIndexNames', type='GENERAL BTREE', " +
                               "schema=(:someLabel {someProperty, someOtherProperty}), indexProvider='native-btree-1.0' ))" );

            assertIndexString( relTypeTokenIndex,
                               "IndexDefinition[relationship type:<any-types>] " +
                               "(Index( id=%d, name='relTypeTokenIndex', type='GENERAL LOOKUP', " +
                               "schema=-[:<any-types>]-, indexProvider='token-1.0' ))" );
            assertIndexString( relTypeProperty,
                               "IndexDefinition[relationship type:someRelationship on:someProperty] " +
                               "(Index( id=%d, name='relTypeIndexName', type='GENERAL BTREE', " +
                               "schema=-[:someRelationship {someProperty}]-, indexProvider='native-btree-1.0' ))" );
            assertIndexString( relTypeProperties,
                               "IndexDefinition[relationship type:someRelationship on:someProperty,someOtherProperty] " +
                               "(Index( id=%d, name='relTypeIndexNames', type='GENERAL BTREE', " +
                               "schema=-[:someRelationship {someProperty, someOtherProperty}]-, indexProvider='native-btree-1.0' ))" );
        }
    }

    private void assertIndexString( IndexDefinition index, String expectedStringFormat )
    {
        assertThat( index.toString() ).isEqualTo( expectedStringFormat, ((IndexDefinitionImpl) index).getIndexReference().getId() );
    }
}
