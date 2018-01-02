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

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

interface StyleConfiguration
{
    void setGraphProperty( String property, String value );

    void setDefaultNodeProperty( String property, String value );

	void setDefaultRelationshipProperty( String property, String value );

    void displayRelationshipLabel( boolean on );

	void setNodePropertyFilter( PropertyFilter filter );

	void setRelationshipPropertyFilter( PropertyFilter filter );

	void setNodeParameterGetter( String key,
	    ParameterGetter<? super Node> getter );

	void setRelationshipParameterGetter( String key,
	    ParameterGetter<? super Relationship> getter );

	void setRelationshipTitleGetter( TitleGetter<? super Relationship> getter );

	void setNodeTitleGetter( TitleGetter<? super Node> getter );

	void setNodePropertyFomatter( PropertyFormatter format );

	void setRelationshipPropertyFomatter( PropertyFormatter format );

	/**
	 * @deprecated use {@link #setRelationshipReverseOrderPredicate(Predicate)} instead
	 */
    @Deprecated
    void setRelationshipReverseOrderPredicate( org.neo4j.helpers.Predicate<Relationship> reversed );

	void setRelationshipReverseOrderPredicate( Predicate<Relationship> reversed );

    String escapeLabel( String label );
}
