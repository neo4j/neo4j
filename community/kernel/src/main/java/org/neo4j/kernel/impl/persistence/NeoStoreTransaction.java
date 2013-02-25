/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.persistence;

import java.util.Map;

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

/**
 * A connection to a {@link PersistenceSource}. <CODE>ResourceConnection</CODE>
 * contains operations to retrieve the {@link javax.transaction.xa.XAResource}
 * for this connection and to close the connection, optionally returning it to a
 * connection pool.
 */
public interface NeoStoreTransaction
{
    void setXaConnection( XaConnection connection );

    /**
     * Destroy this transaction. Makes it not known to anyone.
     */
    void destroy();

    /**
     * Deletes a node by its id, returning its properties which are now removed.
     *
     * @param nodeId The id of the node to delete.
     * @return The properties of the node that were removed during the delete.
     */
    ArrayMap<Integer,PropertyData> nodeDelete( long nodeId );

    /**
     * Adds a property to the given node, with the given index and value.
     *
     * @param nodeId The id of the node to which to add the property.
     * @param index The index of the key of the property to add.
     * @param value The value of the property.
     * @return The added property, as a PropertyData object.
     */
    PropertyData nodeAddProperty( long nodeId, PropertyIndex index, Object value );

    /**
     * Changes an existing property of the given node, with the given index to
     * the passed value
     *
     * @param nodeId The id of the node which holds the property to change.
     * @param index The index of the key of the property to change.
     * @param value The new value of the property.
     * @return The changed property, as a PropertyData object.
     */
    PropertyData nodeChangeProperty( long nodeId, PropertyData index,
            Object value );

    /**
     * Removes the given property identified by indexKeyId of the node with the
     * given id.
     *
     * @param nodeId The id of the node that is to have the property removed.
     * @param index The index key of the property.
     */
    void nodeRemoveProperty( long nodeId, PropertyData index );

    /**
     * Creates a node for the given id
     *
     * @param id The id of the node to create.
     */
    void nodeCreate( long id );

    /**
     * Creates a relationship with the given id, from the nodes identified by id
     * and of type typeId
     *
     * @param id The id of the relationship to create.
     * @param typeId The id of the relationship type this relationship will
     *            have.
     * @param startNodeId The id of the start node.
     * @param endNodeId The id of the end node.
     */
    void relationshipCreate( long id, int typeId, long startNodeId,
        long endNodeId );

    /**
     * Deletes a relationship by its id, returning its properties which are now
     * removed. It is assumed that the nodes it connects have already been
     * deleted in this
     * transaction.
     *
     * @param relId The id of the relationship to delete.
     * @return The properties of the relationship that were removed during the
     *         delete.
     */
    ArrayMap<Integer,PropertyData> relDelete( long relId );

    /**
     * Adds a property to the given relationship, with the given index and
     * value.
     *
     * @param relId The id of the relationship to which to add the property.
     * @param index The index of the key of the property to add.
     * @param value The value of the property.
     * @return The added property, as a PropertyData object.
     */
    PropertyData relAddProperty( long relId, PropertyIndex index, Object value );

    /**
     * Changes an existing property's value of the given relationship, with the
     * given index to the passed value
     *
     * @param relId The id of the relationship which holds the property to
     *            change.
     * @param index The index of the key of the property to change.
     * @param value The new value of the property.
     * @return The changed property, as a PropertyData object.
     */
    PropertyData relChangeProperty( long relId, PropertyData index,
            Object value );

    /**
     * Removes the given property identified by its index from the relationship
     * with the given id.
     *
     * @param relId The id of the relationship that is to have the property
     *            removed.
     * @param index The index key of the property.
     */
    void relRemoveProperty( long relId, PropertyData index );

    /**
     * Tries to load the light node with the given id, returns true on success.
     *
     * @param id The id of the node to load.
     * @return True iff the node record can be found.
     */
    NodeRecord nodeLoadLight( long id );

    /**
     * Attempts to load the value off the store forthe given PropertyData
     * object.
     *
     * @param property The property to make heavy
     * @return The property data
     */
    Object loadPropertyValue( PropertyData property );

    /**
     * Adds a property to the graph, with the given index and value.
     *
     * @param index The index of the key of the property to add.
     * @param value The value of the property.
     * @return The added property, as a PropertyData object.
     */
    PropertyData graphAddProperty( PropertyIndex index, Object value );

    /**
     * Changes an existing property of the graph, with the given index to
     * the passed value
     *
     * @param index The index of the key of the property to change.
     * @param value The new value of the property.
     * @return The changed property, as a PropertyData object.
     */
    PropertyData graphChangeProperty( PropertyData index, Object value );

    /**
     * Removes the given property identified by indexKeyId of the graph with the
     * given id.
     *
     * @param index The index key of the property.
     */
    void graphRemoveProperty( PropertyData index );
    
    /**
     * Loads the complete property chain for the graph and returns it as a
     * map from property index id to property data.
     *
     * @param light If the properties should be loaded light or not.
     * @return The properties loaded, as a map from property index id to
     *         property data.
     */
    ArrayMap<Integer,PropertyData> graphLoadProperties( boolean light );
    
    /**
     * Loads the value object for the given property index record id if the
     * record is light.
     *
     * @param id The id of the property index record to make heavy
     * @return The property index value
     */
    String loadIndex( int id );

    /**
     * Tries to load as heavy records as many property index records as
     * specified in the argument.
     *
     * @return An array of the PropertyIndexData that were loaded - can be less
     *         than the number requested.
     */
    NameData[] loadPropertyIndexes();

    /**
     * Loads the complete property chain for the given node and returns it as a
     * map from property index id to property data.
     *
     * @param nodeId The id of the node whose properties to load.
     * @param light If the properties should be loaded light or not.
     * @return The properties loaded, as a map from property index id to
     *         property data.
     */
    ArrayMap<Integer,PropertyData> nodeLoadProperties( long nodeId, boolean light );

    /**
     * Loads the complete property chain for the given relationship and returns
     * it as a map from property index id to property data.
     *
     * @param relId The id of the relationship whose properties to load.
     * @param light If the properties should be loaded light or not.
     * @return The properties loaded, as a map from property index id to
     *         property data.
     */
    ArrayMap<Integer,PropertyData> relLoadProperties( long relId,
            boolean light);

    /**
     * Tries to load the light relationship with the given id, returns the
     * record on success.
     *
     * @param id The id of the relationship to load.
     * @return The light RelationshipRecord if it was found, null otherwise.
     */
    RelationshipRecord relLoadLight( long id );

    /**
     * Loads and returns all the available RelationshipTypes that are stored.
     *
     * @return All the stored RelationshipTypes, as a RelationshipTypeData array
     */
    NameData[] loadRelationshipTypes();

    /**
     * Creates a property index entry out of the given id and string.
     *
     * @param key The key of the property index, as a string.
     * @param id The property index record id.
     */
    void createPropertyIndex( String key, int id );

    /**
     * Creates a new RelationshipType record with the given id that has the
     * given name.
     *
     * @param id The id of the new relationship type record.
     * @param name The name of the relationship type.
     */
    void createRelationshipType( int id, String name );

    long getRelationshipChainPosition( long nodeId );

    /*
     * List<Iterable<RelationshipRecord>> is a list with three items:
     * 0: outgoing relationships
     * 1: incoming relationships
     * 2: loop relationships
     *
     * Long is the relationship chain position as it stands after this
     * batch of relationships has been loaded.
     */
    Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> getMoreRelationships(
            long nodeId, long position );

    /**
     * Returns an array view of the ids of the nodes that have been created in
     * this transaction.
     *
     * @return An array of the ids of the nodes created in this transaction.
     */
    RelIdArray getCreatedNodes();

    /**
     * Check if the node with the given id was created in this transaction.
     *
     * @param nodeId The node id to check.
     * @return True iff a node with the given id was created in this
     *         transaction.
     */
    boolean isNodeCreated( long nodeId );

    /**
     * Check if the node with the given id was created in this transaction.
     *
     * @param relId The relationship id to check.
     * @return True iff a node with the given id was created in this
     *         transaction.
     */
    boolean isRelationshipCreated( long relId );

    /**
     * Returns the index key ids that are contained within the property record
     * with the specified id.
     *
     * @param property The PropertyData of the property record.
     * @return an array that contains all the property index ids of the blocks
     *         in the record.
     */
    int getKeyIdForProperty( PropertyData property );

    boolean delistResource( Transaction tx, int tmsuccess )
        throws SystemException;
    
    void createSchemaRule( SchemaRule schemaRule );
    
    void dropSchemaRule( long id );
    
    void addLabelToNode( long labelId, long nodeId );
    
    void removeLabelFromNode( long labelId, long nodeId );

    Iterable<Long> getLabelsForNode( long nodeId );
}
