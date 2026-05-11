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
package org.neo4j.cypher.util;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
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
import org.neo4j.internal.kernel.api.RelationshipCursor;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.connectioninfo.RoutingInfo;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.ExceptionHandlerService;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.values.ElementIdMapper;

public class SystemLastTransactionIdTrackingWrapper
        implements InternalTransaction, org.neo4j.cypher.internal.javacompat.InternalTransactionWrapper {

    private final InternalTransaction internalTransaction;
    private final Consumer<Long> systemTransactionIdSetter;
    private final GraphDatabaseAPI db;

    public SystemLastTransactionIdTrackingWrapper(
            InternalTransaction internalTransaction, Consumer<Long> systemTransactionIdSetter, GraphDatabaseAPI db) {
        this.internalTransaction = internalTransaction;
        this.systemTransactionIdSetter = systemTransactionIdSetter;
        this.db = db;
    }

    @Override
    public void setTransaction(KernelTransaction transaction) {
        internalTransaction.setTransaction(transaction);
    }

    @Override
    public KernelTransaction kernelTransaction() {
        return internalTransaction.kernelTransaction();
    }

    @Override
    public KernelTransaction.Type transactionType() {
        return internalTransaction.transactionType();
    }

    @Override
    public SecurityContext securityContext() {
        return internalTransaction.securityContext();
    }

    @Override
    public ClientConnectionInfo clientInfo() {
        return internalTransaction.clientInfo();
    }

    @Override
    public RoutingInfo routingInfo() {
        return internalTransaction.routingInfo();
    }

    @Override
    public List<String> bookmarks() {
        return internalTransaction.bookmarks();
    }

    @Override
    public KernelTransaction.Revertable overrideWith(SecurityContext context) {
        return internalTransaction.overrideWith(context);
    }

    @Override
    public Optional<Status> terminationReason() {
        return internalTransaction.terminationReason();
    }

    @Override
    public void setMetaData(Map<String, Object> txMeta) {
        internalTransaction.setMetaData(txMeta);
    }

    @Override
    public void checkInTransaction() {
        internalTransaction.checkInTransaction();
    }

    @Override
    public boolean isOpen() {
        return internalTransaction.isOpen();
    }

    @Override
    public void terminate(Status reason) {
        internalTransaction.terminate(reason);
    }

    @Override
    public UUID getDatabaseId() {
        return internalTransaction.getDatabaseId();
    }

    @Override
    public String getDatabaseName() {
        return internalTransaction.getDatabaseName();
    }

    @Override
    public Entity validateSameDB(Entity entity) {
        return internalTransaction.validateSameDB(entity);
    }

    @Override
    public ElementIdMapper elementIdMapper() {
        return internalTransaction.elementIdMapper();
    }

    @Override
    public void commit(KernelTransaction.Monitor monitor) {
        var kernelTx = internalTransaction.kernelTransaction();
        internalTransaction.commit(monitor);
        if (kernelTx.getDatabaseName().equalsIgnoreCase("SYSTEM")) {
            systemTransactionIdSetter.accept(db.getDependencyResolver()
                    .resolveDependency(TransactionIdStore.class)
                    .getLastCommittedTransactionId());
        }
    }

    @Override
    public ExceptionHandlerService exceptionHandlerService() {
        return internalTransaction.exceptionHandlerService();
    }

    @Override
    public Node createNode() {
        return internalTransaction.createNode();
    }

    @Override
    public Node createNode(Label... labels) {
        return internalTransaction.createNode(labels);
    }

    @Override
    public Node getNodeById(long id) {
        return internalTransaction.getNodeById(id);
    }

    @Override
    public Node getNodeByElementId(String elementId) {
        return internalTransaction.getNodeByElementId(elementId);
    }

    @Override
    public Relationship getRelationshipById(long id) {
        return internalTransaction.getRelationshipById(id);
    }

    @Override
    public Relationship getRelationshipByElementId(String elementId) {
        return internalTransaction.getRelationshipByElementId(elementId);
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription() {
        return internalTransaction.bidirectionalTraversalDescription();
    }

    @Override
    public TraversalDescription traversalDescription() {
        return internalTransaction.traversalDescription();
    }

    @Override
    public Result execute(String query) throws QueryExecutionException {
        return internalTransaction.execute(query);
    }

    @Override
    public Result execute(String query, Map<String, Object> parameters) throws QueryExecutionException {
        return internalTransaction.execute(query, parameters);
    }

    @Override
    public Iterable<Label> getAllLabelsInUse() {
        return internalTransaction.getAllLabelsInUse();
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        return internalTransaction.getAllRelationshipTypesInUse();
    }

    @Override
    public Iterable<Label> getAllLabels() {
        return internalTransaction.getAllLabels();
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypes() {
        return internalTransaction.getAllRelationshipTypes();
    }

    @Override
    public Iterable<String> getAllPropertyKeys() {
        return internalTransaction.getAllPropertyKeys();
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key, String template, StringSearchMode searchMode) {
        return internalTransaction.findNodes(label, key, template, searchMode);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, Map<String, Object> propertyValues) {
        return internalTransaction.findNodes(label, propertyValues);
    }

    @Override
    public ResourceIterator<Node> findNodes(
            Label label, String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        return internalTransaction.findNodes(label, key1, value1, key2, value2, key3, value3);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2) {
        return internalTransaction.findNodes(label, key1, value1, key2, value2);
    }

    @Override
    public Node findNode(Label label, String key, Object value) {
        return internalTransaction.findNode(label, key, value);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key, Object value) {
        return internalTransaction.findNodes(label, key, value);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label) {
        return internalTransaction.findNodes(label);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, String key, String template, StringSearchMode searchMode) {
        return internalTransaction.findRelationships(relationshipType, key, template, searchMode);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, Map<String, Object> propertyValues) {
        return internalTransaction.findRelationships(relationshipType, propertyValues);
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
        return internalTransaction.findRelationships(relationshipType, key1, value1, key2, value2, key3, value3);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, String key1, Object value1, String key2, Object value2) {
        return internalTransaction.findRelationships(relationshipType, key1, value1, key2, value2);
    }

    @Override
    public Relationship findRelationship(RelationshipType relationshipType, String key, Object value) {
        return internalTransaction.findRelationship(relationshipType, key, value);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, String key, Object value) {
        return internalTransaction.findRelationships(relationshipType, key, value);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType) {
        return internalTransaction.findRelationships(relationshipType);
    }

    @Override
    public void terminate() {
        internalTransaction.terminate();
    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        return internalTransaction.getAllNodes();
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        return internalTransaction.getAllRelationships();
    }

    @Override
    public Lock acquireWriteLock(Entity entity) {
        return internalTransaction.acquireWriteLock(entity);
    }

    @Override
    public Lock acquireReadLock(Entity entity) {
        return internalTransaction.acquireReadLock(entity);
    }

    @Override
    public Schema schema() {
        return internalTransaction.schema();
    }

    @Override
    public void commit() {
        var kernelTx = internalTransaction.kernelTransaction();
        internalTransaction.commit();
        if (kernelTx.getDatabaseName().equalsIgnoreCase("SYSTEM")) {
            systemTransactionIdSetter.accept(db.getDependencyResolver()
                    .resolveDependency(TransactionIdStore.class)
                    .getLastCommittedTransactionId());
        }
    }

    @Override
    public void rollback() {
        internalTransaction.rollback();
    }

    @Override
    public void close() {
        internalTransaction.close();
    }

    @Override
    public void registerCloseableResource(AutoCloseable closeableResource) {
        internalTransaction.registerCloseableResource(closeableResource);
    }

    @Override
    public void unregisterCloseableResource(AutoCloseable closeableResource) {
        internalTransaction.unregisterCloseableResource(closeableResource);
    }

    @Override
    public Relationship newRelationshipEntity(long id) {
        return internalTransaction.newRelationshipEntity(id);
    }

    @Override
    public Relationship newRelationshipEntity(String elementId) {
        return internalTransaction.newRelationshipEntity(elementId);
    }

    @Override
    public Relationship newRelationshipEntity(long id, long startNodeId, int typeId, long endNodeId) {
        return internalTransaction.newRelationshipEntity(id, startNodeId, typeId, endNodeId);
    }

    @Override
    public Relationship newRelationshipEntity(RelationshipCursor cursor) {
        return internalTransaction.newRelationshipEntity(cursor);
    }

    @Override
    public Node newNodeEntity(long nodeId) {
        return internalTransaction.newNodeEntity(nodeId);
    }

    @Override
    public RelationshipType getRelationshipTypeById(int type) {
        return internalTransaction.getRelationshipTypeById(type);
    }

    @Override
    public InternalTransaction getInternalTransaction() {
        return internalTransaction;
    }
}
