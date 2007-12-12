/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.persistence;


import javax.transaction.xa.XAResource;
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.core.RawNodeData;
import org.neo4j.impl.core.RawPropertyData;
import org.neo4j.impl.core.RawPropertyIndex;
import org.neo4j.impl.core.RawRelationshipData;
import org.neo4j.impl.core.RawRelationshipTypeData;

/**
 * A connection to a {@link PersistenceSource}. <CODE>ResourceConnection</CODE>
 * contains operations to retrieve the {@link javax.transaction.xa.XAResource}
 * for this connection and to close the connection, optionally returning it to
 * a connection pool.
 */
public interface ResourceConnection
{
	/**
	 * Returns the {@link javax.transaction.xa.XAResource} that represents
	 * this connection.
	 * @return the <CODE>XAResource</CODE> for this connection
	 */	
	public XAResource getXAResource();
	
	public void destroy();
	
    public void nodeDelete( int nodeId );

    public int nodeAddProperty( int nodeId, PropertyIndex index, Object value );

    public void nodeChangeProperty( int nodeId, int propertyId, Object value );

    public void nodeRemoveProperty( int nodeId, int propertyId );

    public void nodeCreate( int id );

    public void relationshipCreate( int id, int typeId, int startNodeId, 
        int endNodeId );

    public void relDelete( int relId );

    public int relAddProperty( int relId, PropertyIndex index, Object value );

    public void relChangeProperty( int relId, int propertyId, Object value );

    public void relRemoveProperty( int relId, int propertyId );

    public RawNodeData nodeLoadLight( int id );

    public Object loadPropertyValue( int id );

    public String loadIndex( int id );

    public RawPropertyIndex[] loadPropertyIndexes( int maxCount );

    public RawRelationshipData[] nodeLoadRelationships( int nodeId );

    public RawPropertyData[] nodeLoadProperties( int nodeId );

    public RawPropertyData[] relLoadProperties( int relId );

    public RawRelationshipData relLoadLight( int id );

    public RawRelationshipTypeData[] loadRelationshipTypes();

    public void createPropertyIndex( String key, int id );

    public void createRelationshipType( int id, String name );
}