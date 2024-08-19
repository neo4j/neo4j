/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.StringSearchMode;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.connectioninfo.RoutingInfo;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.values.ElementIdMapper;

public class ProcedureTransactionImpl implements InternalTransaction {
    private final InternalTransaction transaction;

    public ProcedureTransactionImpl(InternalTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public void registerCloseableResource(AutoCloseable closeableResource) {
        transaction.registerCloseableResource(closeableResource);
    }

    @Override
    public void unregisterCloseableResource(AutoCloseable closeableResource) {
        transaction.unregisterCloseableResource(closeableResource);
    }

    @Override
    public void commit(KernelTransaction.KernelTransactionMonitor monitor) {
        commit();
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("Commit of ongoing transaction inside of procedure is unsupported.");
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("Rollback of ongoing transaction inside of procedure is unsupported.");
    }

    @Override
    public Node createNode() {
        return transaction.createNode();
    }

    @Override
    public Node createNode(Label... labels) {
        return transaction.createNode(labels);
    }

    @Override
    public Node getNodeById(long id) {
        return transaction.getNodeById(id);
    }

    @Override
    public Node getNodeByElementId(String elementId) {
        return transaction.getNodeByElementId(elementId);
    }

    @Override
    public Result execute(String query) throws QueryExecutionException {
        return transaction.execute(query);
    }

    @Override
    public Result execute(String query, Map<String, Object> parameters) throws QueryExecutionException {
        return transaction.execute(query, parameters);
    }

    @Override
    public Relationship getRelationshipById(long id) {
        return transaction.getRelationshipById(id);
    }

    @Override
    public Relationship getRelationshipByElementId(String elementId) {
        return transaction.getRelationshipByElementId(elementId);
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription() {
        return transaction.bidirectionalTraversalDescription();
    }

    @Override
    public TraversalDescription traversalDescription() {
        return transaction.traversalDescription();
    }

    @Override
    public Iterable<Label> getAllLabelsInUse() {
        return transaction.getAllLabelsInUse();
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        return transaction.getAllRelationshipTypesInUse();
    }

    @Override
    public Iterable<Label> getAllLabels() {
        return transaction.getAllLabels();
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypes() {
        return transaction.getAllRelationshipTypes();
    }

    @Override
    public Iterable<String> getAllPropertyKeys() {
        return transaction.getAllPropertyKeys();
    }

    @Override
    public Node findNode(Label myLabel, String key, Object value) {
        return transaction.findNode(myLabel, key, value);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label myLabel) {
        return transaction.findNodes(myLabel);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, String key, String template, StringSearchMode searchMode) {
        return transaction.findRelationships(relationshipType, key, template, searchMode);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, Map<String, Object> propertyValues) {
        return transaction.findRelationships(relationshipType, propertyValues);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType,
            String key1,
            Object value1,
            String key2,
            Object value2,
            String key3,
            Object value3) {
        return transaction.findRelationships(relationshipType, key1, value1, key2, value2, key3, value3);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, String key1, Object value1, String key2, Object value2) {
        return transaction.findRelationships(relationshipType, key1, value1, key2, value2);
    }

    @Override
    public Relationship findRelationship(RelationshipType relationshipType, String key, Object value) {
        return transaction.findRelationship(relationshipType, key, value);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, String key, Object value) {
        return transaction.findRelationships(relationshipType, key, value);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType) {
        return transaction.findRelationships(relationshipType);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label myLabel, String key, Object value) {
        return transaction.findNodes(myLabel, key, value);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label myLabel, String key, String value, StringSearchMode searchMode) {
        return transaction.findNodes(myLabel, key, value, searchMode);
    }

    @Override
    public ResourceIterator<Node> findNodes(
            Label label, String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        return transaction.findNodes(label, key1, value1, key2, value2, key3, value3);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2) {
        return transaction.findNodes(label, key1, value1, key2, value2);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, Map<String, Object> propertyValues) {
        return transaction.findNodes(label, propertyValues);
    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        return transaction.getAllNodes();
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        return transaction.getAllRelationships();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Close of ongoing transaction inside of procedure is unsupported.");
    }

    @Override
    public void setTransaction(KernelTransaction transaction) {
        this.transaction.setTransaction(transaction);
    }

    @Override
    public Lock acquireWriteLock(Entity entity) {
        return transaction.acquireWriteLock(entity);
    }

    @Override
    public Lock acquireReadLock(Entity entity) {
        return transaction.acquireReadLock(entity);
    }

    @Override
    public Schema schema() {
        return transaction.schema();
    }

    @Override
    public KernelTransaction kernelTransaction() {
        return transaction.kernelTransaction();
    }

    @Override
    public KernelTransaction.Type transactionType() {
        return transaction.transactionType();
    }

    @Override
    public SecurityContext securityContext() {
        return transaction.securityContext();
    }

    @Override
    public ClientConnectionInfo clientInfo() {
        return transaction.clientInfo();
    }

    @Override
    public RoutingInfo routingInfo() {
        return transaction.routingInfo();
    }

    @Override
    public KernelTransaction.Revertable overrideWith(SecurityContext context) {
        return transaction.overrideWith(context);
    }

    @Override
    public Optional<Status> terminationReason() {
        return transaction.terminationReason();
    }

    @Override
    public void setMetaData(Map<String, Object> txMeta) {
        transaction.setMetaData(txMeta);
    }

    @Override
    public void checkInTransaction() {
        transaction.checkInTransaction();
    }

    @Override
    public boolean isOpen() {
        return transaction.isOpen();
    }

    @Override
    public void terminate(Status reason) {
        transaction.terminate(reason);
    }

    @Override
    public UUID getDatabaseId() {
        return transaction.getDatabaseId();
    }

    @Override
    public String getDatabaseName() {
        return transaction.getDatabaseName();
    }

    @Override
    public Entity validateSameDB(Entity entity) {
        return transaction.validateSameDB(entity);
    }

    @Override
    public void terminate() {
        transaction.terminate();
    }

    @Override
    public Relationship newRelationshipEntity(long id) {
        return transaction.newRelationshipEntity(id);
    }

    @Override
    public Relationship newRelationshipEntity(String elementId) {
        return transaction.newRelationshipEntity(elementId);
    }

    @Override
    public Relationship newRelationshipEntity(long id, long startNodeId, int typeId, long endNodeId) {
        return transaction.newRelationshipEntity(id, startNodeId, typeId, endNodeId);
    }

    @Override
    public Relationship newRelationshipEntity(RelationshipDataAccessor cursor) {
        return transaction.newRelationshipEntity(cursor);
    }

    @Override
    public Node newNodeEntity(long nodeId) {
        return transaction.newNodeEntity(nodeId);
    }

    @Override
    public RelationshipType getRelationshipTypeById(int type) {
        return transaction.getRelationshipTypeById(type);
    }

    @Override
    public ElementIdMapper elementIdMapper() {
        return transaction.elementIdMapper();
    }
}
