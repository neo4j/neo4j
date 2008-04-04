package org.neo4j.graphviz;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Traverser;

public abstract class EmittingConsumer
{
	private Set<Relationship> relations = new HashSet<Relationship>();
	private Set<Node> nodes = new HashSet<Node>();

	public void consume( Traverser traverser )
	{
		for ( Node node : traverser )
		{
			visit( node );
		}
		for ( Relationship relation : relations )
		{
			visit( relation );
		}
	}

	private void visit( Node node )
	{
		emit( node );
		nodes.add( node );
		for ( Relationship relation : node.getRelationships() )
		{
			relations.add( relation );
		}
	}

	private void visit( Relationship relation )
	{
		if ( nodes.contains( relation.getStartNode() )
		    && nodes.contains( relation.getEndNode() ) )
		{
			emit( relation );
		}
	}

	private void emit( Node node )
	{
		Emitter emitter = emitNode( node.getId() );
		for ( String key : node.getPropertyKeys() )
		{
			emitter.emitProperty( key, node.getProperty( key ) );
		}
		emitter.done();
	}

	private void emit( Relationship relation )
	{
		long start = relation.getStartNode().getId();
		long end = relation.getEndNode().getId();
		Emitter emitter = emitRelationship( relation.getId(), relation
		    .getType(), start, end );
		for ( String key : relation.getPropertyKeys() )
		{
			emitter.emitProperty( key, relation.getProperty( key ) );
		}
		emitter.done();
	}

	protected abstract Emitter emitRelationship( long id,
	    RelationshipType type, long start, long end );

	protected abstract Emitter emitNode( long id );
}
