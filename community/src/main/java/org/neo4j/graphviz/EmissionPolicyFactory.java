package org.neo4j.graphviz;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

class EmissionPolicyFactory
{
	private static final String EMITTER_POLICY_PREFIX = "neo.emit.policy.";
	private static final String STRING_LIST_SEPARATOR = ",";

	private static enum EmissionProperty
	{
		noNodeProperties
		{
			@Override
			void apply( EmissionPolicyFactory factory, String prop )
			{
				factory.result.acceptedNodeProperties = Collections.EMPTY_SET;
			}
		},
		noRelationshipProperties
		{
			@Override
			void apply( EmissionPolicyFactory factory, String prop )
			{
				factory.result.acceptedRelationshipProperties = Collections.EMPTY_SET;
			}
		},
		noProperties
		{
			@Override
			void apply( EmissionPolicyFactory factory, String prop )
			{
				noNodeProperties.apply( factory, prop );
				noRelationshipProperties.apply( factory, prop );
			}
		},
		acceptedRelationshipProperties
		{
			@Override
			void apply( EmissionPolicyFactory factory, String argument )
			{
				illegalArgumentIf(
				    factory.result.acceptedRelationshipProperties != null,
				    "relationship property restrictions already specified" );
				Set<String> accepted = new HashSet<String>();
				for ( String prop : argument.split( STRING_LIST_SEPARATOR ) )
				{
					accepted.add( prop );
				}
				factory.result.acceptedRelationshipProperties = accepted;
			}
		},
		acceptedNodeProperties
		{
			@Override
			void apply( EmissionPolicyFactory factory, String argument )
			{
				illegalArgumentIf(
				    factory.result.acceptedNodeProperties != null,
				    "node property restrictions already specified" );
				Set<String> accepted = new HashSet<String>();
				for ( String prop : argument.split( STRING_LIST_SEPARATOR ) )
				{
					accepted.add( prop );
				}
				factory.result.acceptedNodeProperties = accepted;
			}
		},
		acceptedProperties
		{
			@Override
			void apply( EmissionPolicyFactory factory, String argument )
			{
				acceptedNodeProperties.apply( factory, argument );
				acceptedRelationshipProperties.apply( factory, argument );
			}
		};
		abstract void apply( EmissionPolicyFactory factory, String prop );

		void illegalArgumentIf( boolean predicate, String message )
		{
			if ( predicate )
			{
				throw new IllegalArgumentException( EMITTER_POLICY_PREFIX
				    + name() + ": " + message );
			}
		}
	}

	static EmissionPolicy buildPolicyFromProperties( Properties properties )
	{
		EmissionPolicyFactory factory = new EmissionPolicyFactory();
		for ( String prop : properties.stringPropertyNames() )
		{
			if ( prop.startsWith( EMITTER_POLICY_PREFIX ) )
			{
				EmissionProperty.valueOf(
				    prop.substring( EMITTER_POLICY_PREFIX.length() ) ).apply(
				    factory, prop );
			}
		}
		return factory.construct();
	}

	private ConstructableEmissionPolicy result = new ConstructableEmissionPolicy();

	private EmissionPolicy construct()
	{
		EmissionPolicy result = this.result;
		this.result = null;
		return result;
	}

	private static class ConstructableEmissionPolicy implements EmissionPolicy
	{
		protected Set<String> acceptedRelationshipProperties;
		protected Set<String> acceptedNodeProperties;

		@Override
		public boolean acceptProperty( SourceType source, String key )
		{
			switch ( source )
			{
				case NODE:
					if ( acceptedNodeProperties != null )
					{
						return acceptedNodeProperties.contains( key );
					}
					else
					{
						return true;
					}
				case RELATIONSHIP:
					if ( acceptedRelationshipProperties != null )
					{
						return acceptedRelationshipProperties.contains( key );
					}
					else
					{
						return true;
					}
				default:
					return false;
			}
		}
	}
}
