/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import javax.transaction.xa.XAResource;

import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipChainPosition;
import org.neo4j.kernel.impl.nioneo.store.RelationshipData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;

/**
 * A connection to a {@link PersistenceSource}. <CODE>ResourceConnection</CODE>
 * contains operations to retrieve the {@link javax.transaction.xa.XAResource}
 * for this connection and to close the connection, optionally returning it to a
 * connection pool.
 */
public interface ResourceConnection
{
    /**
     * Returns the {@link javax.transaction.xa.XAResource} that represents this
     * connection.
     * @return the <CODE>XAResource</CODE> for this connection
     */
    public XAResource getXAResource();

    public void destroy();

    public ArrayMap<Integer,PropertyData> nodeDelete( long nodeId );

    public long nodeAddProperty( long nodeId, PropertyIndex index, Object value );

    public void nodeChangeProperty( long nodeId, long propertyId, Object value );

    public void nodeRemoveProperty( long nodeId, long propertyId );

    public void nodeCreate( long id );

    public void relationshipCreate( long id, int typeId, long startNodeId,
        long endNodeId );

    public ArrayMap<Integer,PropertyData> relDelete( long relId );

    public long relAddProperty( long relId, PropertyIndex index, Object value );

    public void relChangeProperty( long relId, long propertyId, Object value );

    public void relRemoveProperty( long relId, long propertyId );

    public boolean nodeLoadLight( long id );

    public Object loadPropertyValue( long id );

    public String loadIndex( int id );

    public PropertyIndexData[] loadPropertyIndexes( int maxCount );

    public ArrayMap<Integer,PropertyData> nodeLoadProperties( long nodeId,
            boolean light );

    public ArrayMap<Integer,PropertyData> relLoadProperties( long relId,
            boolean light);

    public RelationshipData relLoadLight( long id );

    public RelationshipTypeData[] loadRelationshipTypes();

    public void createPropertyIndex( String key, int id );

    public void createRelationshipType( int id, String name );

    public RelationshipChainPosition getRelationshipChainPosition( long nodeId );

    public Iterable<RelationshipData> getMoreRelationships( long nodeId,
        RelationshipChainPosition position );

    public RelIdArray getCreatedNodes();

    public boolean isNodeCreated( long nodeId );

    public boolean isRelationshipCreated( long relId );

    public int getKeyIdForProperty( long propertyId );
}