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
package org.neo4j.consistency.report;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.store.synthetic.CountsEntry;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
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

public interface ConsistencyReport
{
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface Warning
    {
    }

    @Retention(RetentionPolicy.SOURCE)
    @Target(ElementType.METHOD)
    @interface IncrementalOnly
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
        /** The referenced property record is not in use. */
        @Documented
        void propertyNotInUse( PropertyRecord property );

        /** The referenced property record is not the first in its property chain. */
        @Documented
        void propertyNotFirstInChain( PropertyRecord property );

        /** The referenced property is owned by another Node. */
        @Documented
        void multipleOwners( NodeRecord node );

        /** The referenced property is owned by another Relationship. */
        @Documented
        void multipleOwners( RelationshipRecord relationship );

        /** The referenced property is owned by the neo store (graph global property). */
        @Documented
        void multipleOwners( NeoStoreRecord neoStore );

        /** The first property record reference has changed, but the previous first property record has not been updated. */
        @Documented
        @IncrementalOnly
        void propertyNotUpdated();

        /** The property chain contains multiple properties that have the same property key id, which means that the entity has at least one duplicate property. */
        @Documented
        void propertyKeyNotUniqueInChain();
    }

    interface NeoStoreConsistencyReport extends PrimitiveConsistencyReport
    {
    }

    interface SchemaConsistencyReport extends ConsistencyReport
    {
        /** The label token record referenced from the schema is not in use. */
        @Documented
        void labelNotInUse( LabelTokenRecord label );

        /** The property key token record is not in use. */
        @Documented
        void propertyKeyNotInUse( PropertyKeyTokenRecord propertyKey );

        /** The uniqueness constraint does not reference back to the given record */
        @Documented
        void uniquenessConstraintNotReferencingBack( DynamicRecord ruleRecord );

        /** The constraint index does not reference back to the given record */
        @Documented
        void constraintIndexRuleNotReferencingBack( DynamicRecord ruleRecord );

        /** This record is required to reference some other record of the given kind but no such obligation was found */
        @Documented
        void missingObligation( SchemaRule.Kind kind );

        /**
         * This record requires some other record to reference back to it but there already was such a
         * conflicting obligation created by the record given as a parameter
         */
        @Documented
        void duplicateObligation( DynamicRecord record );

        /**
         * This record contains an index rule which has the same content as the index rule contained in the
         * record given as parameter
         */
        @Documented
        void duplicateRuleContent( DynamicRecord record );

        /** The schema rule contained in the DynamicRecord chain is malformed (not deserializable) */
        @Documented
        void malformedSchemaRule();

        /** The schema rule contained in the DynamicRecord chain is of an unrecognized Kind */
        @Documented
        void unsupportedSchemaRuleKind( SchemaRule.Kind kind );
    }

    interface NodeConsistencyReport extends PrimitiveConsistencyReport
    {
        /** The referenced relationship record is not in use. */
        @Documented
        void relationshipNotInUse( RelationshipRecord referenced );

        /** The referenced relationship record is a relationship between two other nodes. */
        @Documented
        void relationshipForOtherNode( RelationshipRecord relationship );

        /** The referenced relationship record is not the first in the relationship chain where this node is source. */
        @Documented
        void relationshipNotFirstInSourceChain( RelationshipRecord relationship );

        /** The referenced relationship record is not the first in the relationship chain where this node is target. */
        @Documented
        void relationshipNotFirstInTargetChain( RelationshipRecord relationship );

        /** The first relationship record reference has changed, but the previous first relationship record has not been updated. */
        @Documented
        @IncrementalOnly
        void relationshipNotUpdated();

        /** The label token record referenced from a node record is not in use. */
        @Documented
        void labelNotInUse( LabelTokenRecord label );

        /** The label token record is referenced twice from the same node. */
        @Documented
        void labelDuplicate( long labelId );

        /** The label id array is not ordered */
        @Documented
        void labelsOutOfOrder( long largest, long smallest );

        /** The dynamic label record is not in use. */
        @Documented
        void dynamicLabelRecordNotInUse( DynamicRecord record );

        /** This record points to a next record that was already part of this dynamic record chain. */
        @Documented
        void dynamicRecordChainCycle( DynamicRecord nextRecord );

        /** This node was not found in the expected index. */
        @Documented
        void notIndexed( IndexRule index, Object propertyValue );

        /** This node was found in the expected index, although multiple times */
        @Documented
        void indexedMultipleTimes( IndexRule index, Object propertyValue, int count );

        /** There is another node in the unique index with the same property value. */
        @Documented
        void uniqueIndexNotUnique( IndexRule index, Object propertyValue, long duplicateNodeId );

        /** The referenced relationship group record is not in use. */
        @Documented
        void relationshipGroupNotInUse( RelationshipGroupRecord group );

        /** The first relationship group record reference has changed, but the previous first relationship group record has not been updated. */
        @Documented
        @IncrementalOnly
        void relationshipGroupNotUpdated();

        /** The first relationship group record has another node set as owner. */
        @Documented
        void relationshipGroupHasOtherOwner( RelationshipGroupRecord group );
    }

    interface RelationshipConsistencyReport
            extends PrimitiveConsistencyReport
    {
        /** The relationship type field has an illegal value. */
        @Documented
        void illegalRelationshipType();

        /** The relationship type record is not in use. */
        @Documented
        void relationshipTypeNotInUse( RelationshipTypeTokenRecord relationshipType );

        /** The source node field has an illegal value. */
        @Documented
        void illegalSourceNode();

        /** The target node field has an illegal value. */
        @Documented
        void illegalTargetNode();

        /** The source node is not in use. */
        @Documented
        void sourceNodeNotInUse( NodeRecord node );

        /** The target node is not in use. */
        @Documented
        void targetNodeNotInUse( NodeRecord node );

        /** This record should be the first in the source chain, but the source node does not reference this record. */
        @Documented
        void sourceNodeDoesNotReferenceBack( NodeRecord node );

        /** This record should be the first in the target chain, but the target node does not reference this record. */
        @Documented
        void targetNodeDoesNotReferenceBack( NodeRecord node );

        /** The source node does not have a relationship chain. */
        @Documented
        void sourceNodeHasNoRelationships( NodeRecord source );

        /** The target node does not have a relationship chain. */
        @Documented
        void targetNodeHasNoRelationships( NodeRecord source );

        /** The previous record in the source chain is a relationship between two other nodes. */
        @Documented
        void sourcePrevReferencesOtherNodes( RelationshipRecord relationship );

        /** The next record in the source chain is a relationship between two other nodes. */
        @Documented
        void sourceNextReferencesOtherNodes( RelationshipRecord relationship );

        /** The previous record in the target chain is a relationship between two other nodes. */
        @Documented
        void targetPrevReferencesOtherNodes( RelationshipRecord relationship );

        /** The next record in the target chain is a relationship between two other nodes. */
        @Documented
        void targetNextReferencesOtherNodes( RelationshipRecord relationship );

        /** The previous record in the source chain does not have this record as its next record. */
        @Documented
        void sourcePrevDoesNotReferenceBack( RelationshipRecord relationship );

        /** The next record in the source chain does not have this record as its previous record. */
        @Documented
        void sourceNextDoesNotReferenceBack( RelationshipRecord relationship );

        /** The previous record in the target chain does not have this record as its next record. */
        @Documented
        void targetPrevDoesNotReferenceBack( RelationshipRecord relationship );

        /** The next record in the target chain does not have this record as its previous record. */
        @Documented
        void targetNextDoesNotReferenceBack( RelationshipRecord relationship );

        /** The previous source relationship reference has changed, but the previously referenced record has not been updated. */
        @Documented
        @IncrementalOnly
        void sourcePrevNotUpdated();

        /** The next source relationship reference has changed, but the previously referenced record has not been updated. */
        @Documented
        @IncrementalOnly
        void sourceNextNotUpdated();

        /** The previous target relationship reference has changed, but the previously referenced record has not been updated. */
        @Documented
        @IncrementalOnly
        void targetPrevNotUpdated();

        /** The next target relationship reference has changed, but the previously referenced record has not been updated. */
        @Documented
        @IncrementalOnly
        void targetNextNotUpdated();

        /**
         * This relationship was first in the chain for the source node, and isn't first anymore,
         * but the source node was not updated.
         */
        @Documented
        @IncrementalOnly
        void sourceNodeNotUpdated();

        /**
         * This relationship was first in the chain for the target node, and isn't first anymore,
         * but the target node was not updated.
         */
        @Documented
        @IncrementalOnly
        void targetNodeNotUpdated();
    }

    interface PropertyConsistencyReport extends ConsistencyReport
    {
        /** The property key as an invalid value. */
        @Documented
        void invalidPropertyKey( PropertyBlock block );

        /** The key for this property is not in use. */
        @Documented
        void keyNotInUse( PropertyBlock block, PropertyKeyTokenRecord key );

        /** The previous property record is not in use. */
        @Documented
        void prevNotInUse( PropertyRecord property );

        /** The next property record is not in use. */
        @Documented
        void nextNotInUse( PropertyRecord property );

        /** The previous property record does not have this record as its next record. */
        @Documented
        void previousDoesNotReferenceBack( PropertyRecord property );

        /** The next property record does not have this record as its previous record. */
        @Documented
        void nextDoesNotReferenceBack( PropertyRecord property );

        /** The type of this property is invalid. */
        @Documented
        void invalidPropertyType( PropertyBlock block );

        /** The string block is not in use. */
        @Documented
        void stringNotInUse( PropertyBlock block, DynamicRecord value );

        /** The array block is not in use. */
        @Documented
        void arrayNotInUse( PropertyBlock block, DynamicRecord value );

        /** The string block is empty. */
        @Documented
        void stringEmpty( PropertyBlock block, DynamicRecord value );

        /** The array block is empty. */
        @Documented
        void arrayEmpty( PropertyBlock block, DynamicRecord value );

        /** The property value is invalid. */
        @Documented
        void invalidPropertyValue( PropertyBlock block );

        /** This record is first in a property chain, but no Node or Relationship records reference this record. */
        @Documented
        void orphanPropertyChain();

        /** The previous reference has changed, but the referenced record has not been updated. */
        @Documented
        @IncrementalOnly
        void prevNotUpdated();

        /** The next reference has changed, but the referenced record has not been updated. */
        @Documented
        @IncrementalOnly
        void nextNotUpdated();

        /** The string property is not referenced anymore, but the corresponding block has not been deleted. */
        @Documented
        @IncrementalOnly
        void stringUnreferencedButNotDeleted( PropertyBlock block );

        /** The array property is not referenced anymore, but the corresponding block as not been deleted. */
        @Documented
        @IncrementalOnly
        void arrayUnreferencedButNotDeleted( PropertyBlock block );

        /**
         * This property was declared to be changed for a node or relationship, but that node or relationship does not
         * contain this property in its property chain.
         */
        @Documented
        @IncrementalOnly
        void ownerDoesNotReferenceBack();

        /**
         * This property was declared to be changed for a node or relationship, but that node or relationship did not
         * contain this property in its property chain prior to the change. The property is referenced by another owner.
         */
        @Documented
        @IncrementalOnly
        void changedForWrongOwner();

        /** The string record referred from this property is also referred from a another property. */
        @Documented
        void stringMultipleOwners( PropertyRecord otherOwner );

        /** The array record referred from this property is also referred from a another property. */
        @Documented
        void arrayMultipleOwners( PropertyRecord otherOwner );

        /** The string record referred from this property is also referred from a another string record. */
        @Documented
        void stringMultipleOwners( DynamicRecord dynamic );

        /** The array record referred from this property is also referred from a another array record. */
        @Documented
        void arrayMultipleOwners( DynamicRecord dynamic );
    }

    interface NameConsistencyReport extends ConsistencyReport
    {
        /** The name block is not in use. */
        @Documented
        void nameBlockNotInUse( DynamicRecord record );

        /**
         * The token name is empty. Empty token names are discouraged and also prevented in version 2.0.x and above,
         * but they can be accessed just like any other tokens. It's possible that this token have been created
         * in an earlier version where there were no checks for name being empty.
         */
        @Documented
        @Warning
        void emptyName( DynamicRecord name );

        /** The string record referred from this name record is also referred from a another string record. */
        @Documented
        void nameMultipleOwners( DynamicRecord otherOwner );
    }

    interface RelationshipTypeConsistencyReport extends NameConsistencyReport
    {
        /** The string record referred from this relationship type is also referred from a another relationship type. */
        @Documented
        void nameMultipleOwners( RelationshipTypeTokenRecord otherOwner );
    }

    interface LabelTokenConsistencyReport extends NameConsistencyReport
    {
        /** The string record referred from this label name is also referred from a another label name. */
        @Documented
        void nameMultipleOwners( LabelTokenRecord otherOwner );
    }

    interface PropertyKeyTokenConsistencyReport extends NameConsistencyReport
    {
        /** The string record referred from this key is also referred from a another key. */
        @Documented
        void nameMultipleOwners( PropertyKeyTokenRecord otherOwner );
    }

    interface RelationshipGroupConsistencyReport extends ConsistencyReport
    {
        /** The relationship type field has an illegal value. */
        @Documented
        void illegalRelationshipType();

        /** The relationship type record is not in use. */
        @Documented
        void relationshipTypeNotInUse( RelationshipTypeTokenRecord referred );

        /** The next relationship group reference has changed, but the previously referenced record has not been updated. */
        @Documented
        @IncrementalOnly
        void nextNotUpdated();

        /** The next relationship group is not in use. */
        @Documented
        void nextGroupNotInUse();

        /** The location of group in the chain is invalid, should be sorted by type ascending. */
        @Documented
        void invalidTypeSortOrder();

        /** The first outgoing relationship is not in use. */
        @Documented
        void firstOutgoingRelationshipNotInUse();

        /** The first incoming relationship is not in use. */
        @Documented
        void firstIncomingRelationshipNotInUse();

        /** The first loop relationship is not in use. */
        @Documented
        void firstLoopRelationshipNotInUse();

        /** The first outgoing relationship is not the first in its chain. */
        @Documented
        void firstOutgoingRelationshipNotFirstInChain();

        /** The first incoming relationship is not the first in its chain. */
        @Documented
        void firstIncomingRelationshipNotFirstInChain();

        /** The first loop relationship is not the first in its chain. */
        @Documented
        void firstLoopRelationshipNotFirstInChain();

        /** The first outgoing relationship is of a different type than its group. */
        @Documented
        void firstOutgoingRelationshipOfOfOtherType();

        /** The first incoming relationship is of a different type than its group. */
        @Documented
        void firstIncomingRelationshipOfOfOtherType();

        /** The first loop relationship is of a different type than its group. */
        @Documented
        void firstLoopRelationshipOfOfOtherType();

        /** The owner of the relationship group is not in use. */
        @Documented
        void ownerNotInUse();

        /** Illegal owner value. */
        @Documented
        void illegalOwner();

        /** Next chained relationship group has another owner. */
        @Documented
        void nextHasOtherOwner( RelationshipGroupRecord referred );
    }

    interface DynamicConsistencyReport extends ConsistencyReport
    {
        /** The next block is not in use. */
        @Documented
        void nextNotInUse( DynamicRecord next );

        /** The record is not full, but references a next block. */
        @Documented
        @Warning
        void recordNotFullReferencesNext();

        /** The length of the block is invalid. */
        @Documented
        void invalidLength();

        /** The block is empty. */
        @Documented
        @Warning
        void emptyBlock();

        /** The next block is empty. */
        @Documented
        @Warning
        void emptyNextBlock( DynamicRecord next );

        /** The next block references this (the same) record. */
        @Documented
        void selfReferentialNext();

        /** The next block reference was changed, but the previously referenced block was not updated. */
        @Documented
        @IncrementalOnly
        void nextNotUpdated();

        /** The next block of this record is also referenced by another dynamic record. */
        @Documented
        void nextMultipleOwners( DynamicRecord otherOwner );

        /** The next block of this record is also referenced by a property record. */
        @Documented
        void nextMultipleOwners( PropertyRecord otherOwner );

        /** The next block of this record is also referenced by a relationship type. */
        @Documented
        void nextMultipleOwners( RelationshipTypeTokenRecord otherOwner );

        /** The next block of this record is also referenced by a property key. */
        @Documented
        void nextMultipleOwners( PropertyKeyTokenRecord otherOwner );

        /** This record not referenced from any other dynamic block, or from any property or name record. */
        @Documented
        void orphanDynamicRecord();
    }

    interface DynamicLabelConsistencyReport extends ConsistencyReport
    {
        /** This label record is not referenced by its owning node record or that record is not in use. */
        @Documented
        void orphanDynamicLabelRecordDueToInvalidOwner( NodeRecord owningNodeRecord );

        /** This label record does not have an owning node record. */
        @Documented
        void orphanDynamicLabelRecord();
    }

    interface NodeInUseWithCorrectLabelsReport extends ConsistencyReport
    {
        void nodeNotInUse( NodeRecord referredNodeRecord );

        void nodeDoesNotHaveExpectedLabel( NodeRecord referredNodeRecord, long expectedLabelId );
    }

    interface LabelScanConsistencyReport extends NodeInUseWithCorrectLabelsReport
    {
        /** This label scan document refers to a node record that is not in use. */
        @Override
        @Documented
        void nodeNotInUse( NodeRecord referredNodeRecord );

        /** This label scan document refers to a node that does not have the expected label. */
        @Override
        @Documented
        void nodeDoesNotHaveExpectedLabel( NodeRecord referredNodeRecord, long expectedLabelId );
    }

    interface IndexConsistencyReport extends NodeInUseWithCorrectLabelsReport
    {
        /** This index entry refers to a node record that is not in use. */
        @Override
        @Documented
        void nodeNotInUse( NodeRecord referredNodeRecord );

        /** This index entry refers to a node that does not have the expected label. */
        @Override
        @Documented
        void nodeDoesNotHaveExpectedLabel( NodeRecord referredNodeRecord, long expectedLabelId );
    }

    interface LabelsMatchReport extends ConsistencyReport
    {
        /** This node record has a label that is not found in the label scan store entry for this node */
        @Documented
        void nodeLabelNotInIndex( NodeRecord referredNodeRecord, long missingLabelId );
    }

    public interface CountsConsistencyReport extends ConsistencyReport
    {
        /** The node count does not correspond with the expected count. */
        @Documented
        void inconsistentNodeCount( long expectedCount );

        /** The relationship count does not correspond with the expected count. */
        @Documented
        void inconsistentRelationshipCount( long expectedCount );

        /** The node key entries in the store does not correspond with the expected number. */
        @Documented
        void inconsistentNumberOfNodeKeys( long expectedCount );

        /** The relationship key entries in the store does not correspond with the expected number. */
        @Documented
        void inconsistentNumberOfRelationshipKeys( long expectedCount );
    }
}
