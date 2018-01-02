/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.legacy.consistency.report;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.legacy.consistency.RecordType;
import org.neo4j.legacy.consistency.checking.RecordCheck;
import org.neo4j.legacy.consistency.store.synthetic.CountsEntry;
import org.neo4j.legacy.consistency.store.synthetic.IndexEntry;
import org.neo4j.legacy.consistency.store.synthetic.LabelScanDocument;

public interface ConsistencyReport
{
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface Warning
    {
    }

    public interface Reporter
    {
        void forSchema( DynamicRecord schema,
                        RecordCheck<DynamicRecord, SchemaConsistencyReport> checker );

        void forSchemaChange( DynamicRecord oldSchema, DynamicRecord newSchema,
                              RecordCheck<DynamicRecord, SchemaConsistencyReport> checker );

        void forNode( NodeRecord node,
                      RecordCheck<NodeRecord, NodeConsistencyReport> checker );

        void forNodeChange( NodeRecord oldNode, NodeRecord newNode,
                            RecordCheck<NodeRecord, NodeConsistencyReport> checker );

        void forRelationship( RelationshipRecord relationship,
                              RecordCheck<RelationshipRecord, RelationshipConsistencyReport> checker );

        void forRelationshipChange( RelationshipRecord oldRelationship, RelationshipRecord newRelationship,
                                    RecordCheck<RelationshipRecord, RelationshipConsistencyReport> checker );

        void forProperty( PropertyRecord property,
                          RecordCheck<PropertyRecord, PropertyConsistencyReport> checker );

        void forPropertyChange( PropertyRecord oldProperty, PropertyRecord newProperty,
                                RecordCheck<PropertyRecord, PropertyConsistencyReport> checker );

        void forRelationshipTypeName( RelationshipTypeTokenRecord relationshipType,
                                      RecordCheck<RelationshipTypeTokenRecord, RelationshipTypeConsistencyReport> checker );

        void forRelationshipTypeNameChange( RelationshipTypeTokenRecord oldType, RelationshipTypeTokenRecord newType,
                                            RecordCheck<RelationshipTypeTokenRecord, RelationshipTypeConsistencyReport> checker );

        void forLabelName( LabelTokenRecord label,
                           RecordCheck<LabelTokenRecord, LabelTokenConsistencyReport> checker );

        void forLabelNameChange( LabelTokenRecord oldLabel, LabelTokenRecord newLabel,
                           RecordCheck<LabelTokenRecord, LabelTokenConsistencyReport> checker );

        void forPropertyKey( PropertyKeyTokenRecord key,
                             RecordCheck<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport> checker );

        void forPropertyKeyChange( PropertyKeyTokenRecord oldKey, PropertyKeyTokenRecord newKey,
                                   RecordCheck<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport> checker );

        void forDynamicBlock( RecordType type, DynamicRecord record,
                              RecordCheck<DynamicRecord, DynamicConsistencyReport> checker );

        void forDynamicBlockChange( RecordType type, DynamicRecord oldRecord, DynamicRecord newRecord,
                                    RecordCheck<DynamicRecord, DynamicConsistencyReport> checker );


        void forDynamicLabelBlock( RecordType type, DynamicRecord record,
                                   RecordCheck<DynamicRecord, DynamicLabelConsistencyReport> checker );

        void forDynamicLabelBlockChange( RecordType type, DynamicRecord oldRecord, DynamicRecord newRecord,
                                         RecordCheck<DynamicRecord, DynamicLabelConsistencyReport> checker );

        void forNodeLabelScan( LabelScanDocument document,
                               RecordCheck<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport> checker );

        void forIndexEntry( IndexEntry entry,
                            RecordCheck<IndexEntry, ConsistencyReport.IndexConsistencyReport> checker );

        void forNodeLabelMatch( NodeRecord nodeRecord, RecordCheck<NodeRecord, LabelsMatchReport> nodeLabelCheck );

        void forRelationshipGroup( RelationshipGroupRecord record,
                RecordCheck<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> checker );

        void forRelationshipGroupChange( RelationshipGroupRecord oldRecord, RelationshipGroupRecord newRecord,
                RecordCheck<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> checker );

        void forCounts( CountsEntry countsEntry,
                        RecordCheck<CountsEntry, ConsistencyReport.CountsConsistencyReport> checker );
    }

    interface PrimitiveConsistencyReport extends ConsistencyReport
    {
        @Documented( "The referenced property record is not in use." )
        void propertyNotInUse( PropertyRecord property );

        @Documented( "The referenced property record is not the first in its property chain." )
        void propertyNotFirstInChain( PropertyRecord property );

        @Documented( "The referenced property is owned by another Node." )
        void multipleOwners( NodeRecord node );

        @Documented( "The referenced property is owned by another Relationship." )
        void multipleOwners( RelationshipRecord relationship );

        @Documented( "The referenced property is owned by the neo store (graph global property)." )
        void multipleOwners( NeoStoreRecord neoStore );

        @Documented( "The first property record reference has changed, but the previous first property record has not been updated." )
        void propertyNotUpdated();

        @Documented( "The property chain contains multiple properties that have the same property key id, which means that the entity has at least one duplicate property." )
        void propertyKeyNotUniqueInChain();

        @Documented( "The property chain does not contain a property that is mandatory for this entity." )
        void missingMandatoryProperty( int key );
    }

    interface NeoStoreConsistencyReport extends PrimitiveConsistencyReport
    {
    }

    interface SchemaConsistencyReport extends ConsistencyReport
    {
        @Documented( "The label token record referenced from the schema is not in use." )
        void labelNotInUse( LabelTokenRecord label );

        @Documented( "The relationship type token record referenced from the schema is not in use." )
        void relationshipTypeNotInUse( RelationshipTypeTokenRecord relationshipType );

        @Documented( "The property key token record is not in use." )
        void propertyKeyNotInUse( PropertyKeyTokenRecord propertyKey );

        @Documented( "The uniqueness constraint does not reference back to the given record" )
        void uniquenessConstraintNotReferencingBack( DynamicRecord ruleRecord );

        @Documented( "The constraint index does not reference back to the given record" )
        void constraintIndexRuleNotReferencingBack( DynamicRecord ruleRecord );

        @Documented( "This record is required to reference some other record of the given kind but no such obligation was found" )
        void missingObligation( SchemaRule.Kind kind );

        @Documented( "This record requires some other record to reference back to it but there already was such a conflicting obligation created by the record given as a parameter" )
        void duplicateObligation( DynamicRecord record );

        @Documented( "This record contains a schema rule which has the same content as the schema rule contained in" )
        void duplicateRuleContent(DynamicRecord record );

        @Documented( "The schema rule contained in the DynamicRecord chain is malformed (not deserializable)" )
        void malformedSchemaRule();

        @Documented( "The schema rule contained in the DynamicRecord chain is of an unrecognized Kind" )
        void unsupportedSchemaRuleKind( SchemaRule.Kind kind );
    }

    interface NodeConsistencyReport extends PrimitiveConsistencyReport
    {
        @Documented( "The referenced relationship record is not in use." )
        void relationshipNotInUse( RelationshipRecord referenced );

        @Documented( "The referenced relationship record is a relationship between two other nodes." )
        void relationshipForOtherNode( RelationshipRecord relationship );

        @Documented( "The referenced relationship record is not the first in the relationship chain where this node is source." )
        void relationshipNotFirstInSourceChain( RelationshipRecord relationship );

        @Documented( "The referenced relationship record is not the first in the relationship chain where this node is target." )
        void relationshipNotFirstInTargetChain( RelationshipRecord relationship );

        @Documented( "The first relationship record reference has changed, but the previous first relationship record has not been updated." )
        void relationshipNotUpdated();

        @Documented( "The label token record referenced from a node record is not in use." )
        void labelNotInUse( LabelTokenRecord label );

        @Documented( "The label token record is referenced twice from the same node." )
        void labelDuplicate( long labelId );

        @Documented( "The label id array is not ordered" )
        void labelsOutOfOrder( long largest, long smallest );

        @Documented( "The dynamic label record is not in use." )
        void dynamicLabelRecordNotInUse( DynamicRecord record );

        @Documented( "This record points to a next record that was already part of this dynamic record chain." )
        void dynamicRecordChainCycle( DynamicRecord nextRecord );

        @Documented( "This node was not found in the expected index." )
        void notIndexed( IndexRule index, Object propertyValue );

        @Documented( "This node was found in the expected index, although multiple times" )
        void indexedMultipleTimes( IndexRule index, Object propertyValue, int count );

        @Documented( "There is another node in the unique index with the same property value." )
        void uniqueIndexNotUnique( IndexRule index, Object propertyValue, long duplicateNodeId );

        @Documented( "The referenced relationship group record is not in use." )
        void relationshipGroupNotInUse( RelationshipGroupRecord group );

        @Documented( "The first relationship group record reference has changed, but the previous first relationship group record has not been updated." )
        void relationshipGroupNotUpdated();

        @Documented( "The first relationship group record has another node set as owner." )
        void relationshipGroupHasOtherOwner( RelationshipGroupRecord group );
    }

    interface RelationshipConsistencyReport
            extends PrimitiveConsistencyReport
    {
        @Documented( "The relationship type field has an illegal value." )
        void illegalRelationshipType();

        @Documented( "The relationship type record is not in use." )
        void relationshipTypeNotInUse( RelationshipTypeTokenRecord relationshipType );

        @Documented( "The source node field has an illegal value." )
        void illegalSourceNode();

        @Documented( "The target node field has an illegal value." )
        void illegalTargetNode();

        @Documented( "The source node is not in use." )
        void sourceNodeNotInUse( NodeRecord node );

        @Documented( "The target node is not in use." )
        void targetNodeNotInUse( NodeRecord node );

        @Documented( "This record should be the first in the source chain, but the source node does not reference this record." )
        void sourceNodeDoesNotReferenceBack( NodeRecord node );

        @Documented( "This record should be the first in the target chain, but the target node does not reference this record." )
        void targetNodeDoesNotReferenceBack( NodeRecord node );

        @Documented( "The source node does not have a relationship chain." )
        void sourceNodeHasNoRelationships( NodeRecord source );

        @Documented( "The target node does not have a relationship chain." )
        void targetNodeHasNoRelationships( NodeRecord source );

        @Documented( "The previous record in the source chain is a relationship between two other nodes." )
        void sourcePrevReferencesOtherNodes( RelationshipRecord relationship );

        @Documented( "The next record in the source chain is a relationship between two other nodes." )
        void sourceNextReferencesOtherNodes( RelationshipRecord relationship );

        @Documented( "The previous record in the target chain is a relationship between two other nodes." )
        void targetPrevReferencesOtherNodes( RelationshipRecord relationship );

        @Documented( "The next record in the target chain is a relationship between two other nodes." )
        void targetNextReferencesOtherNodes( RelationshipRecord relationship );

        @Documented( "The previous record in the source chain does not have this record as its next record." )
        void sourcePrevDoesNotReferenceBack( RelationshipRecord relationship );

        @Documented( "The next record in the source chain does not have this record as its previous record." )
        void sourceNextDoesNotReferenceBack( RelationshipRecord relationship );

        @Documented( "The previous record in the target chain does not have this record as its next record." )
        void targetPrevDoesNotReferenceBack( RelationshipRecord relationship );

        @Documented( "The next record in the target chain does not have this record as its previous record." )
        void targetNextDoesNotReferenceBack( RelationshipRecord relationship );

        @Documented( "The previous source relationship reference has changed, but the previously referenced record has not been updated." )
        void sourcePrevNotUpdated();

        @Documented( "The next source relationship reference has changed, but the previously referenced record has not been updated." )
        void sourceNextNotUpdated();

        @Documented( "The previous target relationship reference has changed, but the previously referenced record has not been updated." )
        void targetPrevNotUpdated();

        @Documented( "The next target relationship reference has changed, but the previously referenced record has not been updated." )
        void targetNextNotUpdated();

        @Documented( "This relationship was first in the chain for the source node, and isn't first anymore, but the source node was not updated." )
        void sourceNodeNotUpdated();

        @Documented( "This relationship was first in the chain for the target node, and isn't first anymore, but the target node was not updated." )
        void targetNodeNotUpdated();
    }

    interface PropertyConsistencyReport extends ConsistencyReport
    {
        @Documented( "The property key as an invalid value." )
        void invalidPropertyKey( PropertyBlock block );

        @Documented( "The key for this property is not in use." )
        void keyNotInUse( PropertyBlock block, PropertyKeyTokenRecord key );

        @Documented( "The previous property record is not in use." )
        void prevNotInUse( PropertyRecord property );

        @Documented( "The next property record is not in use." )
        void nextNotInUse( PropertyRecord property );

        @Documented( "The previous property record does not have this record as its next record." )
        void previousDoesNotReferenceBack( PropertyRecord property );

        @Documented( "The next property record does not have this record as its previous record." )
        void nextDoesNotReferenceBack( PropertyRecord property );

        @Documented( "The type of this property is invalid." )
        void invalidPropertyType( PropertyBlock block );

        @Documented( "The string block is not in use." )
        void stringNotInUse( PropertyBlock block, DynamicRecord value );

        @Documented( "The array block is not in use." )
        void arrayNotInUse( PropertyBlock block, DynamicRecord value );

        @Documented( "The string block is empty." )
        void stringEmpty( PropertyBlock block, DynamicRecord value );

        @Documented( "The array block is empty." )
        void arrayEmpty( PropertyBlock block, DynamicRecord value );

        @Documented( "The property value is invalid." )
        void invalidPropertyValue( PropertyBlock block );

        @Documented( "This record is first in a property chain, but no Node or Relationship records reference this record." )
        void orphanPropertyChain();

        @Documented( "The previous reference has changed, but the referenced record has not been updated." )
        void prevNotUpdated();

        @Documented( "The next reference has changed, but the referenced record has not been updated." )
        void nextNotUpdated();

        @Documented( "The string property is not referenced anymore, but the corresponding block has not been deleted." )
        void stringUnreferencedButNotDeleted( PropertyBlock block );

        @Documented( "The array property is not referenced anymore, but the corresponding block as not been deleted." )
        void arrayUnreferencedButNotDeleted( PropertyBlock block );

        @Documented( "This property was declared to be changed for a node or relationship, but that node or relationship does not contain this property in its property chain." )
        void ownerDoesNotReferenceBack();

        @Documented( "This property was declared to be changed for a node or relationship, but that node or relationship did not contain this property in its property chain prior to the change. The property is referenced by another owner." )
        void changedForWrongOwner();

        @Documented( "The string record referred from this property is also referred from a another property." )
        void stringMultipleOwners( PropertyRecord otherOwner );

        @Documented( "The array record referred from this property is also referred from a another property." )
        void arrayMultipleOwners( PropertyRecord otherOwner );

        @Documented( "The string record referred from this property is also referred from a another string record." )
        void stringMultipleOwners( DynamicRecord dynamic );

        @Documented( "The array record referred from this property is also referred from a another array record." )
        void arrayMultipleOwners( DynamicRecord dynamic );
    }

    interface NameConsistencyReport extends ConsistencyReport
    {
        @Documented( "The name block is not in use." )
        void nameBlockNotInUse( DynamicRecord record );

        @Warning
        @Documented( "The token name is empty. Empty token names are discouraged and also prevented in version 2.0.x and above, but they can be accessed just like any other tokens. It's possible that this token have been created in an earlier version where there were no checks for name being empty." )
        void emptyName( DynamicRecord name );

        @Documented( "The string record referred from this name record is also referred from a another string record." )
        void nameMultipleOwners( DynamicRecord otherOwner );
    }

    interface RelationshipTypeConsistencyReport extends NameConsistencyReport
    {
        @Documented( "The string record referred from this relationship type is also referred from a another relationship type." )
        void nameMultipleOwners( RelationshipTypeTokenRecord otherOwner );
    }

    interface LabelTokenConsistencyReport extends NameConsistencyReport
    {
        @Documented( "The string record referred from this label name is also referred from a another label name." )
        void nameMultipleOwners( LabelTokenRecord otherOwner );
    }

    interface PropertyKeyTokenConsistencyReport extends NameConsistencyReport
    {
        @Documented( "The string record referred from this key is also referred from a another key." )
        void nameMultipleOwners( PropertyKeyTokenRecord otherOwner );
    }

    interface RelationshipGroupConsistencyReport extends ConsistencyReport
    {
        @Documented( "The relationship type field has an illegal value." )
        void illegalRelationshipType();

        @Documented( "The relationship type record is not in use." )
        void relationshipTypeNotInUse( RelationshipTypeTokenRecord referred );

        @Documented( "The next relationship group reference has changed, but the previously referenced record has not been updated." )
        void nextNotUpdated();

        @Documented( "The next relationship group is not in use." )
        void nextGroupNotInUse();

        @Documented( "The location of group in the chain is invalid, should be sorted by type ascending." )
        void invalidTypeSortOrder();

        @Documented( "The first outgoing relationship is not in use." )
        void firstOutgoingRelationshipNotInUse();

        @Documented( "The first incoming relationship is not in use." )
        void firstIncomingRelationshipNotInUse();

        @Documented( "The first loop relationship is not in use." )
        void firstLoopRelationshipNotInUse();

        @Documented( "The first outgoing relationship is not the first in its chain." )
        void firstOutgoingRelationshipNotFirstInChain();

        @Documented( "The first incoming relationship is not the first in its chain." )
        void firstIncomingRelationshipNotFirstInChain();

        @Documented( "The first loop relationship is not the first in its chain." )
        void firstLoopRelationshipNotFirstInChain();

        @Documented( "The first outgoing relationship is of a different type than its group." )
        void firstOutgoingRelationshipOfOfOtherType();

        @Documented( "The first incoming relationship is of a different type than its group." )
        void firstIncomingRelationshipOfOfOtherType();

        @Documented( "The first loop relationship is of a different type than its group." )
        void firstLoopRelationshipOfOfOtherType();

        @Documented( "The owner of the relationship group is not in use." )
        void ownerNotInUse();

        @Documented( "Illegal owner value." )
        void illegalOwner();

        @Documented( "Next chained relationship group has another owner." )
        void nextHasOtherOwner( RelationshipGroupRecord referred );
    }

    interface DynamicConsistencyReport extends ConsistencyReport
    {
        @Documented( "The next block is not in use." )
        void nextNotInUse( DynamicRecord next );

        @Warning
        @Documented( "The record is not full, but references a next block." )
        void recordNotFullReferencesNext();

        @Documented( "The length of the block is invalid." )
        void invalidLength();

        @Warning
        @Documented( "The block is empty." )
        void emptyBlock();

        @Documented( "The next block is empty." )
        @Warning
        void emptyNextBlock( DynamicRecord next );

        @Documented( "The next block references this (the same) record." )
        void selfReferentialNext();

        @Documented( "The next block reference was changed, but the previously referenced block was not updated." )
        void nextNotUpdated();

        @Documented( "The next block of this record is also referenced by another dynamic record." )
        void nextMultipleOwners( DynamicRecord otherOwner );

        @Documented( "The next block of this record is also referenced by a property record." )
        void nextMultipleOwners( PropertyRecord otherOwner );

        @Documented( "The next block of this record is also referenced by a relationship type." )
        void nextMultipleOwners( RelationshipTypeTokenRecord otherOwner );

        @Documented( "The next block of this record is also referenced by a property key." )
        void nextMultipleOwners( PropertyKeyTokenRecord otherOwner );

        @Documented( "This record not referenced from any other dynamic block, or from any property or name record." )
        void orphanDynamicRecord();
    }

    interface DynamicLabelConsistencyReport extends ConsistencyReport
    {
        @Documented( "This label record is not referenced by its owning node record or that record is not in use." )
        void orphanDynamicLabelRecordDueToInvalidOwner( NodeRecord owningNodeRecord );

        @Documented( "This label record does not have an owning node record." )
        void orphanDynamicLabelRecord();
    }

    interface NodeInUseWithCorrectLabelsReport extends ConsistencyReport
    {
        void nodeNotInUse( NodeRecord referredNodeRecord );

        void nodeDoesNotHaveExpectedLabel( NodeRecord referredNodeRecord, long expectedLabelId );
    }

    interface LabelScanConsistencyReport extends NodeInUseWithCorrectLabelsReport
    {
        @Override
        @Documented( "This label scan document refers to a node record that is not in use." )
        void nodeNotInUse( NodeRecord referredNodeRecord );

        @Override
        @Documented( "This label scan document refers to a node that does not have the expected label." )
        void nodeDoesNotHaveExpectedLabel( NodeRecord referredNodeRecord, long expectedLabelId );
    }

    interface IndexConsistencyReport extends NodeInUseWithCorrectLabelsReport
    {
        @Override
        @Documented( "This index entry refers to a node record that is not in use." )
        void nodeNotInUse( NodeRecord referredNodeRecord );

        @Override
        @Documented( "This index entry refers to a node that does not have the expected label." )
        void nodeDoesNotHaveExpectedLabel( NodeRecord referredNodeRecord, long expectedLabelId );
    }

    interface LabelsMatchReport extends ConsistencyReport
    {
        @Documented( "This node record has a label that is not found in the label scan store entry for this node" )
        void nodeLabelNotInIndex( NodeRecord referredNodeRecord, long missingLabelId );
    }

    public interface CountsConsistencyReport extends ConsistencyReport
    {
        @Documented( "The node count does not correspond with the expected count." )
        void inconsistentNodeCount( long expectedCount );

        @Documented( "The relationship count does not correspond with the expected count." )
        void inconsistentRelationshipCount( long expectedCount );

        @Documented( "The node key entries in the store does not correspond with the expected number." )
        void inconsistentNumberOfNodeKeys( long expectedCount );

        @Documented( "The relationship key entries in the store does not correspond with the expected number." )
        void inconsistentNumberOfRelationshipKeys( long expectedCount );
    }
}
