package org.neo4j.impl.core;

/**
 * This interface defines methods needed for persistence layer when 
 * incremental changes to a node are made persistent. Nodes generates
 * a pro-active event when changes are made to them and passes event data
 * of this type.
 */
public interface NodeOperationEventData
{
	/**
	 * Returns the node that generated the event. This method is used in 
	 * all persistence operation and should not return null.
	 * 
	 * @return the node
	 */
	public int getNodeId();
	
	/**
	 * Returns the unique id for the property (if it is a property change / 
	 * remove operation). Some persistence sources needs this and some 
	 * don't. If the persistence source needs this id it should load and 
	 * set it when the property is loaded. If not, <CODE>-1</CODE> is returned 
	 * (also the case if not a change / remove property operation).
	 *
	 * @return the property id of the property to be changed/removed
	 */
	public int getPropertyId();
	
	/**
	 * Sets the unique id for the property. This is used as a callback when a 
	 * new property is created from persistence sources that needs to have a 
	 * property id. 
	 *
	 * @param id new property id
	 */
	public void setNewPropertyId( int id );

	/**
	 * Returns the property key name if the operation is a property 
	 * related. Property related operations are add, remove and change 
	 * property. Returns null if not property operation (create delete node).
	 *
	 * @return the property index
	 */
	public String getPropertyKey();
	
	/**
	 * Returns the value of a added property or new value in case of change
	 * property operation, else null.
	 *
	 * @return the property value
	 */
	public Object getProperty();

	/**
	 * Returns the removed value of a added property or old value in case of 
	 * change property operation, else null.
	 *
	 * @return the property value
	 */
	public Object getOldProperty();
}