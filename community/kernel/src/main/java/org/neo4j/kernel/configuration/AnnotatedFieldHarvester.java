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
package org.neo4j.kernel.configuration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Pair;

public class AnnotatedFieldHarvester {

	/**
	 * Find all static fields of a given type, annotated with some given
	 * annotation.
	 * @param clazz
	 * @param type
	 * @param annotation
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> Iterable<Pair<Field, T>> findStatic(Class<?> clazz,
			Class<T> type, Class annotation) {
		List<Pair<Field, T>> found = new ArrayList<Pair<Field, T>>();
		for( Field field : clazz.getDeclaredFields() )
        {
            try
            {
                field.setAccessible( true );

                Object fieldValue = field.get( null );
                if(type.isInstance(fieldValue) && 
                		(annotation == null || field.getAnnotation(annotation) != null)) 
                {
                	found.add(Pair.<Field, T>of(field, (T)fieldValue));
                }
            } catch( IllegalAccessException e )
            {
                assert false : "Field " + clazz.getName() + "#" +field.getName()+" is not public";
            } catch( NullPointerException npe )
            {
            	assert false : "Field " + clazz.getName() + "#" +field.getName()+" is not static";
            }
        }
		
		return found;
	}
	
	/**
	 * Find all static fields of a given type.
	 * @param clazz
	 * @param type
	 */
	public <T> Iterable<Pair<Field, T>> findStatic(Class<?> clazz,
			Class<T> type) {
		return findStatic(clazz, type, null);
	}

}
