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
package org.neo4j.kernel.impl.store.format;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.id.BatchingIdSequence;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;

@ExtendWith( RandomExtension.class )
public abstract class AbstractRecordCloningTest
{
    @Inject
    private RandomRule random;

    private RecordKeys keys;
    private IdSequence idSequence;
    private RecordGenerators.Generator<NodeRecord> nodes;
    private RecordGenerators.Generator<DynamicRecord> dynamics;
    private RecordGenerators.Generator<LabelTokenRecord> labelTokens;
    private RecordGenerators.Generator<PropertyRecord> properties;
    private RecordGenerators.Generator<PropertyKeyTokenRecord> propertyKeyTokens;
    private RecordGenerators.Generator<RelationshipRecord> relationships;
    private RecordGenerators.Generator<RelationshipGroupRecord> relationshipGroups;
    private RecordGenerators.Generator<RelationshipTypeTokenRecord> relationshipTypeTokens;
    private RecordFormat<NodeRecord> nodeFormat;
    private RecordFormat<DynamicRecord> dynamicFormat;
    private RecordFormat<LabelTokenRecord> labelTokenFormat;
    private RecordFormat<PropertyRecord> propertyFormat;
    private RecordFormat<PropertyKeyTokenRecord> propertyKeyTokenFormat;
    private RecordFormat<RelationshipRecord> relationshipFormat;
    private RecordFormat<RelationshipGroupRecord> relationshipGroupFormat;
    private RecordFormat<RelationshipTypeTokenRecord> relationshipTypeTokenFormat;
    private int nodeRecordSize;
    private int dynamicRecordSize;
    private int labelTokenRecordSize;
    private int propertyRecordSize;
    private int propertyKeyTokenRecordSize;
    private int relationshipRecordSize;
    private int relationshipGroupRecordSize;
    private int relationshipTypeTokenRecordSize;

    protected abstract RecordFormats formats();

    protected abstract int entityBits();

    protected abstract int propertyBits();

    @BeforeEach
    private void setUp()
    {
        RecordFormats formats = formats();
        RecordGenerators generators = new LimitedRecordGenerators( random.randomValues(), entityBits(), propertyBits(), 40, 16, -1 );
        keys = FullyCoveringRecordKeys.INSTANCE;
        idSequence = new BatchingIdSequence( 1 );
        nodes = generators.node();
        dynamics = generators.dynamic();
        labelTokens = generators.labelToken();
        properties = generators.property();
        propertyKeyTokens = generators.propertyKeyToken();
        relationships = generators.relationship();
        relationshipGroups = generators.relationshipGroup();
        relationshipTypeTokens = generators.relationshipTypeToken();

        dynamicFormat = formats.dynamic();
        labelTokenFormat = formats.labelToken();
        nodeFormat = formats.node();
        propertyFormat = formats.property();
        propertyKeyTokenFormat = formats.propertyKeyToken();
        relationshipFormat = formats.relationship();
        relationshipGroupFormat = formats.relationshipGroup();
        relationshipTypeTokenFormat = formats.relationshipTypeToken();

        dynamicRecordSize = dynamicFormat.getRecordSize( new IntStoreHeader( GraphDatabaseSettings.DEFAULT_BLOCK_SIZE ) );
        labelTokenRecordSize = labelTokenFormat.getRecordSize( NO_STORE_HEADER );
        nodeRecordSize = nodeFormat.getRecordSize( NO_STORE_HEADER );
        propertyRecordSize = propertyFormat.getRecordSize( NO_STORE_HEADER );
        propertyKeyTokenRecordSize = propertyKeyTokenFormat.getRecordSize( NO_STORE_HEADER );
        relationshipRecordSize = relationshipFormat.getRecordSize( NO_STORE_HEADER );
        relationshipGroupRecordSize = relationshipGroupFormat.getRecordSize( new IntStoreHeader( 50 ) );
        relationshipTypeTokenRecordSize = relationshipTypeTokenFormat.getRecordSize( NO_STORE_HEADER );
    }

    @RepeatedTest( 1000 )
    void plainDynamicClone()
    {
        DynamicRecord dynamicRecord = getDynamicRecord();
        keys.dynamic().assertRecordsEquals( dynamicRecord, dynamicRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void preparedDynamicClone()
    {
        DynamicRecord dynamicRecord = getDynamicRecord();
        dynamicFormat.prepare( dynamicRecord, dynamicRecordSize, idSequence );
        keys.dynamic().assertRecordsEquals( dynamicRecord, dynamicRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void plainLabelTokenClone()
    {
        LabelTokenRecord labelTokenRecord = getLabelTokenRecord();
        keys.labelToken().assertRecordsEquals( labelTokenRecord, labelTokenRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void preparedLabelTokenClone()
    {
        LabelTokenRecord labelTokenRecord = getLabelTokenRecord();
        labelTokenFormat.prepare( labelTokenRecord, labelTokenRecordSize, idSequence );
        keys.labelToken().assertRecordsEquals( labelTokenRecord, labelTokenRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void plainNodeClone()
    {
        NodeRecord nodeRecord = getNodeRecord();
        keys.node().assertRecordsEquals( nodeRecord, nodeRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void preparedNodeClone()
    {
        NodeRecord nodeRecord = getNodeRecord();
        nodeFormat.prepare( nodeRecord, nodeRecordSize, idSequence );
        keys.node().assertRecordsEquals( nodeRecord, nodeRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void plainNodeWithDynamicLabelsClone()
    {
        NodeRecord nodeRecord = getNodeRecord();
        nodeRecord.setLabelField( 12, Arrays.asList( getDynamicRecord(), getDynamicRecord() ) );
        keys.node().assertRecordsEquals( nodeRecord, nodeRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void preparedNodeWithDynamicLabelsClone()
    {
        NodeRecord nodeRecord = getNodeRecord();
        nodeRecord.setLabelField( 12, Arrays.asList( getDynamicRecord(), getDynamicRecord() ) );
        nodeFormat.prepare( nodeRecord, nodeRecordSize, idSequence );
        keys.node().assertRecordsEquals( nodeRecord, nodeRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void plainPropertyClone()
    {
        PropertyRecord propertyRecord = getPropertyRecord();
        keys.property().assertRecordsEquals( propertyRecord, propertyRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void preparedPropertyClone()
    {
        PropertyRecord propertyRecord = getPropertyRecord();
        propertyFormat.prepare( propertyRecord, propertyRecordSize, idSequence );
        keys.property().assertRecordsEquals( propertyRecord, propertyRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void plainPropertyKeyTokenClone()
    {
        PropertyKeyTokenRecord propertyKeyTokenRecord = getPropertyKeyTokenRecord();
        keys.propertyKeyToken().assertRecordsEquals( propertyKeyTokenRecord, propertyKeyTokenRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void preparedPropertyKeyTokenClone()
    {
        PropertyKeyTokenRecord propertyKeyTokenRecord = getPropertyKeyTokenRecord();
        propertyKeyTokenFormat.prepare( propertyKeyTokenRecord, propertyKeyTokenRecordSize, idSequence );
        keys.propertyKeyToken().assertRecordsEquals( propertyKeyTokenRecord, propertyKeyTokenRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void plainRelationshipClone()
    {
        RelationshipRecord relationshipRecord = getRelationshipRecord();
        keys.relationship().assertRecordsEquals( relationshipRecord, relationshipRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void preparedRelationshipClone()
    {
        RelationshipRecord relationshipRecord = getRelationshipRecord();
        relationshipFormat.prepare( relationshipRecord, relationshipRecordSize, idSequence );
        keys.relationship().assertRecordsEquals( relationshipRecord, relationshipRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void plainRelationshipGroupClone()
    {
        RelationshipGroupRecord relationshipGroupRecord = getRelationshipGroupRecord();
        keys.relationshipGroup().assertRecordsEquals( relationshipGroupRecord, relationshipGroupRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void preparedRelationshipGroupClone()
    {
        RelationshipGroupRecord relationshipGroupRecord = getRelationshipGroupRecord();
        relationshipGroupFormat.prepare( relationshipGroupRecord, relationshipGroupRecordSize, idSequence );
        keys.relationshipGroup().assertRecordsEquals( relationshipGroupRecord, relationshipGroupRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void plainRelationshipTypeTokenClone()
    {
        RelationshipTypeTokenRecord relationshipTypeTokenRecord = getRelationshipTypeTokenRecord();
        keys.relationshipTypeToken().assertRecordsEquals( relationshipTypeTokenRecord, relationshipTypeTokenRecord.clone() );
    }

    @RepeatedTest( 1000 )
    void preparedRelationshipTypeTokenClone()
    {
        RelationshipTypeTokenRecord relationshipTypeTokenRecord = getRelationshipTypeTokenRecord();
        relationshipTypeTokenFormat.prepare( relationshipTypeTokenRecord, relationshipTypeTokenRecordSize, idSequence );
        keys.relationshipTypeToken().assertRecordsEquals( relationshipTypeTokenRecord, relationshipTypeTokenRecord.clone() );
    }

    private DynamicRecord getDynamicRecord()
    {
        return dynamics.get( dynamicRecordSize, dynamicFormat, random.nextLong( 1, dynamicFormat.getMaxId() ) );
    }

    private LabelTokenRecord getLabelTokenRecord()
    {
        return labelTokens.get( labelTokenRecordSize, labelTokenFormat, random.nextLong( 1, labelTokenFormat.getMaxId() ) );
    }

    private NodeRecord getNodeRecord()
    {
        return nodes.get( nodeRecordSize, nodeFormat, random.nextLong( 1, nodeFormat.getMaxId() ) );
    }

    private PropertyRecord getPropertyRecord()
    {
        return properties.get( propertyRecordSize, propertyFormat, random.nextLong( 1, propertyFormat.getMaxId() ) );
    }

    private PropertyKeyTokenRecord getPropertyKeyTokenRecord()
    {
        return propertyKeyTokens.get( propertyKeyTokenRecordSize, propertyKeyTokenFormat, random.nextLong( 1, propertyKeyTokenFormat.getMaxId() ) );
    }

    private RelationshipRecord getRelationshipRecord()
    {
        return relationships.get( relationshipRecordSize, relationshipFormat, random.nextLong( 1, relationshipFormat.getMaxId() ) );
    }

    private RelationshipGroupRecord getRelationshipGroupRecord()
    {
        return relationshipGroups.get( relationshipGroupRecordSize, relationshipGroupFormat, random.nextLong( 1, relationshipGroupFormat.getMaxId() ) );
    }

    private RelationshipTypeTokenRecord getRelationshipTypeTokenRecord()
    {
        return relationshipTypeTokens.get( relationshipTypeTokenRecordSize, relationshipTypeTokenFormat,
                random.nextLong( 1, relationshipTypeTokenFormat.getMaxId() ) );
    }
}
