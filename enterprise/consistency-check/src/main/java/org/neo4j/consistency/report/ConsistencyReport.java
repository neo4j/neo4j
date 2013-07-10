/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.report;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.TokenRecord;

public interface ConsistencyReport<RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
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
    }

    <REFERRED extends AbstractBaseRecord> void forReference( RecordReference<REFERRED> other,
                                                             ComparativeRecordChecker<RECORD, ? super REFERRED, REPORT> checker );

    interface PrimitiveConsistencyReport<RECORD extends PrimitiveRecord, REPORT extends PrimitiveConsistencyReport<RECORD, REPORT>>
            extends ConsistencyReport<RECORD, REPORT>
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
    }

    interface NeoStoreConsistencyReport extends PrimitiveConsistencyReport<NeoStoreRecord, NeoStoreConsistencyReport>
    {
    }

    interface SchemaConsistencyReport extends ConsistencyReport<DynamicRecord, SchemaConsistencyReport>
    {
        /** The label token record is not in use. */
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

    interface NodeConsistencyReport extends PrimitiveConsistencyReport<NodeRecord, NodeConsistencyReport>
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

        /** The first relationship record reference has changed, but the previous first relationship record has not been updates. */
        @Documented
        @IncrementalOnly
        void relationshipNotUpdated();

        /** The label token record is not in use. */
        @Documented
        void labelNotInUse( LabelTokenRecord label );

        /** The label token record is referenced twice from the same node. */
        @Documented
        void labelDuplicate( long labelId );

        /** The label token record next block is referencing a record that was already visited as part of this chain. */
        @Documented
        void cyclicDynamicLabelRecords( DynamicRecord record );

        /** The label token record is not in use. */
        @Documented
        void dynamicLabelRecordNotInUse( DynamicRecord record );
    }

    interface RelationshipConsistencyReport
            extends PrimitiveConsistencyReport<RelationshipRecord, RelationshipConsistencyReport>
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

    interface PropertyConsistencyReport extends ConsistencyReport<PropertyRecord, PropertyConsistencyReport>
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

    interface NameConsistencyReport<RECORD extends TokenRecord, REPORT extends NameConsistencyReport<RECORD,REPORT>>
            extends ConsistencyReport<RECORD,REPORT>
    {
        /** The name block is not in use. */
        @Documented
        void nameBlockNotInUse( DynamicRecord record );

        /** The name is empty. */
        @Documented
        @Warning
        void emptyName( DynamicRecord name );

        /** The string record referred from this name record is also referred from a another string record. */
        @Documented
        void nameMultipleOwners( DynamicRecord otherOwner );
    }

    interface RelationshipTypeConsistencyReport extends NameConsistencyReport<RelationshipTypeTokenRecord, RelationshipTypeConsistencyReport>
    {
        /** The string record referred from this relationship type is also referred from a another relationship type. */
        @Documented
        void nameMultipleOwners( RelationshipTypeTokenRecord otherOwner );
    }

    interface LabelTokenConsistencyReport extends NameConsistencyReport<LabelTokenRecord, LabelTokenConsistencyReport>
    {
        /** The string record referred from this label name is also referred from a another label name. */
        @Documented
        void nameMultipleOwners( LabelTokenRecord otherOwner );
    }

    interface PropertyKeyTokenConsistencyReport extends NameConsistencyReport<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport>
    {
        /** The string record referred from this key is also referred from a another key. */
        @Documented
        void nameMultipleOwners( PropertyKeyTokenRecord otherOwner );
    }

    interface DynamicConsistencyReport extends ConsistencyReport<DynamicRecord, DynamicConsistencyReport>
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
}
