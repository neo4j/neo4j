/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.visualization.graphviz;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.visualization.PropertyType;

/**
 * A configuration parameter for the Neo Graphviz system.
 */
public interface StyleParameter
{
	/**
	 * Apply this configuration parameter to a configuration.
	 * @param configuration
	 *            the configuration to apply this parameter to.
	 */
	void configure( StyleConfiguration configuration );

    final class GraphLabel implements StyleParameter
    {
        private final String label;

        public GraphLabel( String label )
        {
            this.label = label;
        }

        public void configure( StyleConfiguration configuration )
        {
            configuration.setGraphProperty( "label", configuration.escapeLabel( label ) );
        }
    }

	/** Configure the font of nodes. */
	final class NodeFont implements StyleParameter
	{
		private final String name;
		private final int size;

		/**
		 * Configure the font of nodes.
		 * @param fontName
		 *            the name of the node font.
		 * @param fontSize
		 *            the size of the node font.
		 */
		public NodeFont( String fontName, int fontSize )
		{
			this.name = fontName;
			this.size = fontSize;
		}

		public final void configure( StyleConfiguration configuration )
		{
			configuration.setDefaultNodeProperty( "fontname", name );
			configuration.setDefaultNodeProperty( "fontsize", Integer
			    .toString( size ) );
		}
	}
	/** Configure the font of relationships. */
	final class RelationshipFont implements StyleParameter
	{
		private final String name;
		private final int size;

		/**
		 * Configure the font of relationships.
		 * @param fontName
		 *            the name of the relationship font.
		 * @param fontSize
		 *            the size of the relationship font.
		 */
		public RelationshipFont( String fontName, int fontSize )
		{
			this.name = fontName;
			this.size = fontSize;
		}

		public final void configure( StyleConfiguration configuration )
		{
			configuration.setDefaultRelationshipProperty( "fontname", name );
			configuration.setDefaultRelationshipProperty( "fontsize", Integer
			    .toString( size ) );
		}
	}
	/** Add a property to the general node configuration. */
	final class DefaultNodeProperty implements StyleParameter
	{
		private final String property;
		private final String value;

		/**
		 * Add a property to the general node configuration.
		 * @param property
		 *            the property key.
		 * @param value
		 *            the property value.
		 */
		public DefaultNodeProperty( String property, String value )
		{
			this.property = property;
			this.value = value;
		}

		public final void configure( StyleConfiguration configuration )
		{
			configuration.setDefaultNodeProperty( property, value );
		}
	}
	/** Add a property to the general relationship configuration. */
	final class DefaultRelationshipProperty implements StyleParameter
	{
		private final String property;
		private final String value;

		/**
		 * Add a property to the general relationship configuration.
		 * @param property
		 *            the property key.
		 * @param value
		 *            the property value.
		 */
		public DefaultRelationshipProperty( String property, String value )
		{
			this.property = property;
			this.value = value;
		}

		public final void configure( StyleConfiguration configuration )
		{
			configuration.setDefaultRelationshipProperty( property, value );
		}
	}
	/** Apply a color to a relationship. */
	abstract class RelationshipColor implements StyleParameter
	{
		public final void configure( StyleConfiguration configuration )
		{
			ParameterGetter<Relationship> getter = new ParameterGetter<Relationship>()
			{
				public String getParameterValue( Relationship relationship,
				    String key )
				{
                    if ( key.equals( "color" ) )
                    {
                        return getColor( relationship );
                    }
                    else
                    {
                        return getFontColor( relationship );
                    }
				}
			};
            configuration.setRelationshipParameterGetter( "color", getter );
            configuration.setRelationshipParameterGetter( "fontcolor", getter );
		}

		/**
		 * Get the font color for the given relationship.
		 * @param relationship
		 *            the relationship to get the font color for.
		 * @return the name of the font color for the given relationship.
		 */
		protected String getFontColor( Relationship relationship )
		{
            return getColor( relationship );
		}

		/**
		 * Get the color for a given relationship.
		 * @param relationship
		 *            the relationship to get the color for.
		 * @return the name of the color for the given relationship.
		 */
		protected abstract String getColor( Relationship relationship );
	}
	/** Apply a color to a relationship based on the type of the relationship. */
	abstract class RelationshipTypeColor extends RelationshipColor
	{
		private final Map<String, String> colors = new HashMap<String, String>();
		private final Map<String, String> fontColors = new HashMap<String, String>();

		@Override
		protected final String getColor( Relationship relationship )
		{
			RelationshipType type = relationship.getType();
			String result = colors.get( type.name() );
			if ( result == null )
			{
				result = getColor( type );
				if ( result == null )
				{
					result = "black";
				}
				colors.put( type.name(), result );
			}
			return result;
		}

		@Override
		protected final String getFontColor( Relationship relationship )
		{
			RelationshipType type = relationship.getType();
			String result = fontColors.get( type.name() );
			if ( result == null )
			{
				result = getFontColor( type );
				if ( result == null )
				{
					result = "black";
				}
				fontColors.put( type.name(), result );
			}
			return result;
		}

		/**
		 * Get the font color for a relationship type. Only invoked once for
		 * each relationship type in the graph.
		 * @param type
		 *            the relationship type to get the font color for.
		 * @return the name of the font color for the relationship type.
		 */
		protected String getFontColor( RelationshipType type )
		{
            return getColor( type );
		}

		/**
		 * Get the color for a relationship type. Only invoked once for each
		 * relationship type in the graph.
		 * @param type
		 *            the relationship type to get the color for.
		 * @return the name of the color for the relationship type.
		 */
		protected abstract String getColor( RelationshipType type );
	}
	/** Apply a color to a node. */
	abstract class NodeColor implements StyleParameter
	{
		public final void configure( StyleConfiguration configuration )
		{
			ParameterGetter<Node> getter = new ParameterGetter<Node>()
			{
				public String getParameterValue( Node node, String key )
				{
					if ( key.equals( "color" ) )
					{
						return getColor( node );
					}
					else if ( key.equals( "fontcolor" ) )
					{
						return getFontColor( node );
					}
					else
					{
						return getFillColor( node );
					}
				}
			};
			configuration.setDefaultNodeProperty( "style", "filled" );
			configuration.setNodeParameterGetter( "color", getter );
			configuration.setNodeParameterGetter( "fillcolor", getter );
			configuration.setNodeParameterGetter( "fontcolor", getter );
		}

		/**
		 * Get the font color for the given node.
		 * @param node
		 *            the node to get the font color for.
		 * @return the name of the font color for the given node.
		 */
		protected String getFontColor( Node node )
		{
            return getColor( node );
		}

		/**
		 * Return the default color for the node. This is the color of the
		 * borders of the node.
		 * @param node
		 *            the node to get the color for.
		 * @return the name of the color for the node.
		 */
		protected abstract String getColor( Node node );

		/**
		 * Return the fill color for the node. This is the color of the interior
		 * of the node.
		 * @param node
		 *            the node to get the color for.
		 * @return the name of the color for the node.
		 */
        protected String getFillColor( Node node )
        {
            return null;
        }
	}
    /**
     * Reverse the logical order of relationships.
     *
     * This affects how the layout of the nodes.
     */
    abstract class ReverseRelationshipOrder implements StyleParameter
    {
        public void configure( StyleConfiguration configuration )
        {
            configuration.setRelationshipReverseOrderPredicate( new Predicate<Relationship>()
            {
                public boolean test( Relationship item )
                {
                    return reversedOrder( item );
                }
            } );
        }

        protected abstract boolean reversedOrder( Relationship edge );
    }
    /** Reverse the logical order of relationships with specific types. */
    final class ReverseOrderRelationshipTypes extends ReverseRelationshipOrder
    {
        private final Set<String> reversedTypes = new HashSet<String>();

        public ReverseOrderRelationshipTypes( RelationshipType... types )
        {
            for ( RelationshipType type : types )
            {
                reversedTypes.add( type.name() );
            }
        }

        @Override
        protected boolean reversedOrder( Relationship edge )
        {
            return reversedTypes.contains( edge.getType().name() );
        }
    }
	/** Add a custom title to nodes. */
	abstract class NodeTitle implements StyleParameter, TitleGetter<Node>
	{
		public final void configure( StyleConfiguration configuration )
		{
			configuration.setNodeTitleGetter( this );
		}
	}
	/** Add a custom title to relationships. */
	abstract class RelationshipTitle implements StyleParameter,
	    TitleGetter<Relationship>
	{
		public final void configure( StyleConfiguration configuration )
		{
			configuration.setRelationshipTitleGetter( this );
		}
	}
	/** Get node title from a property. */
	final class NodeTitleProperty extends NodeTitle
	{
		private final String key;

		/**
		 * Get node title from a property.
		 * @param key
		 *            the property key to use as title.
		 */
		public NodeTitleProperty( String key )
		{
			this.key = key;
		}

		public String getTitle( Node node )
		{
			return ( String ) node.getProperty( key );
		}
	}
	/** Get relationship title from a property. */
	final class RelationshipTitleProperty extends RelationshipTitle
	{
		private final String key;

		/**
		 * Get relationship title from a property.
		 * @param key
		 *            the property key to use as title.
		 */
		public RelationshipTitleProperty( String key )
		{
			this.key = key;
		}

		public String getTitle( Relationship relationship )
		{
			return ( String ) relationship.getProperty( key );
		}
	}
	/** Add custom generic parameters to nodes. */
	abstract class GenericNodeParameters implements StyleParameter,
	    ParameterGetter<Node>
	{
		/**
		 * Add custom generic parameters to nodes.
		 * @param keys
		 *            the parameters to add.
		 */
		protected GenericNodeParameters( String... keys )
		{
			this.keys = Arrays.asList( keys );
		}

		private final Iterable<String> keys;

		public final void configure( StyleConfiguration configuration )
		{
			for ( String key : keys )
			{
				configuration.setNodeParameterGetter( key, this );
			}
		}
	}
	/** Add custom generic parameters to relationships. */
	abstract class GenericRelationshipParameters implements StyleParameter,
	    ParameterGetter<Relationship>
	{
		/**
		 * Add custom generic parameters to relationships.
		 * @param keys
		 *            the parameters to add.
		 */
		protected GenericRelationshipParameters( String... keys )
		{
			this.keys = keys;
		}

		private final String[] keys;

		public final void configure( StyleConfiguration configuration )
		{
			for ( String key : keys )
			{
				configuration.setRelationshipParameterGetter( key, this );
			}
		}
	}
	/** Filter which properties are allowed for nodes. */
	abstract class NodePropertyFilter implements StyleParameter, PropertyFilter
	{
		public final void configure( StyleConfiguration configuration )
		{
			configuration.setNodePropertyFilter( this );
		}
	}
	/** Filter which properties are alloed for relationships. */
	abstract class RelationshipPropertyFilter implements StyleParameter,
	    PropertyFilter
	{
		public final void configure( StyleConfiguration configuration )
		{
			configuration.setRelationshipPropertyFilter( this );
		}
	}
	/** Specify a custom format for node properties. */
	abstract class NodePropertyFormat implements StyleParameter,
	    PropertyFormatter
	{
		public final void configure( StyleConfiguration configuration )
		{
			configuration.setNodePropertyFomatter( this );
		}
	}
	/** Specify a custom format for relationship properties. */
	abstract class RelationshipPropertyFormat implements StyleParameter,
	    PropertyFormatter
	{
		public final void configure( StyleConfiguration configuration )
		{
			configuration.setRelationshipPropertyFomatter( this );
		}
	}
	/** Specify a label for the head of relationships. */
	abstract class RelationshipHeadLabel implements StyleParameter
	{
		public void configure( StyleConfiguration configuration )
		{
			configuration.setRelationshipParameterGetter( "headlabel",
			    new ParameterGetter<Relationship>()
			    {
				    public String getParameterValue( Relationship container,
				        String key )
				    {
					    return getHeadLabel( container );
				    }
			    } );
		}

		/**
		 * Get the head label for a relationship.
		 * @param relationship
		 *            the relationship to get the head label for.
		 * @return the head label for the relationship.
		 */
		protected abstract String getHeadLabel( Relationship relationship );
	}
	/** Specify a label for the tail of relationships. */
	abstract class RelationshipTailLabel implements StyleParameter
	{
		public void configure( StyleConfiguration configuration )
		{
			configuration.setRelationshipParameterGetter( "taillabel",
			    new ParameterGetter<Relationship>()
			    {
				    public String getParameterValue( Relationship container,
				        String key )
				    {
					    return getTailLabel( container );
				    }
			    } );
		}

		/**
		 * Get the tail label for a relationship.
		 * @param relationship
		 *            the relationship to get the tail label for.
		 * @return the tail label for the relationship.
		 */
		protected abstract String getTailLabel( Relationship relationship );
	}
	/**
	 * Simple style parameter that neither requires parameters, nor custom code.
	 */
	enum Simple implements StyleParameter
	{
		/** Don't render properties for relationships. */
		NO_RELATIONSHIP_PROPERTIES
		{
			@Override
			public final void configure( StyleConfiguration configuration )
			{
				configuration
				    .setRelationshipPropertyFilter( new PropertyFilter()
				    {
					    public boolean acceptProperty( String key )
					    {
						    return false;
					    }
				    } );
			}
		},
		/** Don't render labels for relationships. */
		NO_RELATIONSHIP_LABEL
		{
			@Override
			public final void configure( StyleConfiguration configuration )
			{
				configuration.displayRelationshipLabel( false );
			}
		},
		/** Render properties for relationships as "key = value : type". */
		PROPERTY_AS_KEY_EQUALS_VALUE_COLON_TYPE
		{
			@Override
			public final void configure( final StyleConfiguration configuration )
			{
				PropertyFormatter format = new PropertyFormatter()
				{
					public String format( String key, PropertyType type,
					    Object value )
					{
						return configuration.escapeLabel( key ) + " = " + configuration.escapeLabel(PropertyType.format( value ))
						    + " : " + type.typeName;
					}
				};
				configuration.setNodePropertyFomatter( format );
				configuration.setRelationshipPropertyFomatter( format );
			}
		},
		/** Render properties for relationships as "key = value". */
		PROPERTY_AS_KEY_EQUALS_VALUE
		{
			@Override
            public final void configure( final StyleConfiguration configuration )
			{
				PropertyFormatter format = new PropertyFormatter()
				{
					public String format( String key, PropertyType type,
					    Object value )
					{
                        return configuration.escapeLabel( key )
                               + " = "
                               + configuration.escapeLabel( PropertyType.format( value ) );
					}
				};
				configuration.setNodePropertyFomatter( format );
				configuration.setRelationshipPropertyFomatter( format );
			}
		},
		/** Render properties for relationships as "key : type". */
		PROPERTY_AS_KEY_COLON_TYPE
		{
			@Override
            public final void configure( final StyleConfiguration configuration )
			{
				PropertyFormatter format = new PropertyFormatter()
				{
					public String format( String key, PropertyType type,
					    Object value )
					{
                        return configuration.escapeLabel( key ) + " : "
                               + type.typeName;
					}
				};
				configuration.setNodePropertyFomatter( format );
				configuration.setRelationshipPropertyFomatter( format );
			}
		};
		/**
		 * @see StyleParameter#configure(StyleConfiguration)
		 * @param configuration
		 *            same as in
		 *            {@link StyleParameter#configure(StyleConfiguration)}.
		 */
		public abstract void configure( StyleConfiguration configuration );
	}
}
