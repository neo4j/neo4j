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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;

class IndexPrototypeTest
{
    @Test
    void mustGenerateReasonableNames()
    {
        IndexConfig indexConfig = IndexConfig.empty();
        LabelSchemaDescriptor labelSchema = SchemaDescriptor.forLabel( 1, 2, 3 );
        RelationTypeSchemaDescriptor relTypeSchema = SchemaDescriptor.forRelType( 1, 2, 3 );
        FulltextSchemaDescriptor fulltextNodeSchema = SchemaDescriptor.fulltext( NODE, indexConfig, new int[]{1, 2}, new int[]{1, 2} );
        FulltextSchemaDescriptor fulltextRelSchema = SchemaDescriptor.fulltext( RELATIONSHIP, indexConfig, new int[]{1, 2}, new int[]{1, 2} );
        IndexPrototype labelPrototype = IndexPrototype.forSchema( labelSchema );
        IndexPrototype labelUniquePrototype = IndexPrototype.uniqueForSchema( labelSchema );
        IndexPrototype relTypePrototype = IndexPrototype.forSchema( relTypeSchema );
        IndexPrototype relTypeUniquePrototype = IndexPrototype.uniqueForSchema( relTypeSchema );
        IndexPrototype nodeFtsPrototype = IndexPrototype.forSchema( fulltextNodeSchema );
        IndexPrototype relFtsPrototype = IndexPrototype.forSchema( fulltextRelSchema );

        assertName( labelPrototype, new String[]{"A"}, new String[]{"B", "C"}, "Index on :A (B,C)" );
        assertName( labelUniquePrototype, new String[]{"A"}, new String[]{"B", "C"}, "Unique Index on :A (B,C)" );
        assertName( relTypePrototype, new String[]{"A"}, new String[]{"B", "C"}, "Index on ()-[:A]-() (B,C)" );
        assertName( relTypeUniquePrototype, new String[]{"A"}, new String[]{"B", "C"}, "Unique Index on ()-[:A]-() (B,C)" );
        assertName( nodeFtsPrototype, new String[]{"A", "B"}, new String[]{"C", "D"}, "Full-Text Index on :A,:B (C,D)" );
        assertName( relFtsPrototype, new String[]{"A", "B"}, new String[]{"C", "D"}, "Full-Text Index on ()-[:A|:B]-() (C,D)" );
    }

    private void assertName( IndexPrototype labelPrototype, String[] entityTokenNames, String[] propertyNames, String expectedName )
    {
        IndexPrototype withName = labelPrototype.withName( SchemaRule.generateName( labelPrototype, entityTokenNames, propertyNames ) );
        assertTrue( withName.getName().isPresent() );
        assertEquals( expectedName, withName.getName().get() );
    }
}
