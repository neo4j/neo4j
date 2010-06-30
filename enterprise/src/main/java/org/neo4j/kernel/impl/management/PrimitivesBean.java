package org.neo4j.kernel.impl.management;

import javax.management.NotCompliantMBeanException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.management.Primitives;

class PrimitivesBean extends Neo4jMBean implements Primitives
{
    private final NodeManager nodeManager;

    PrimitivesBean( int instanceId, NodeManager nodeManager ) throws NotCompliantMBeanException
    {
        super( instanceId, Primitives.class );
        this.nodeManager = nodeManager;
    }

    public long getNumberOfNodeIdsInUse()
    {
        return nodeManager.getNumberOfIdsInUse( Node.class );
    }

    public long getNumberOfRelationshipIdsInUse()
    {
        return nodeManager.getNumberOfIdsInUse( Relationship.class );
    }


    public long getNumberOfPropertyIdsInUse()
    {
        return nodeManager.getNumberOfIdsInUse( PropertyStore.class );
    }

    public long getNumberOfRelationshipTypeIdsInUse()
    {
        return nodeManager.getNumberOfIdsInUse( RelationshipType.class );
    }
}
