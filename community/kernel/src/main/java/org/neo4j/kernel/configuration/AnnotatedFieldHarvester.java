package org.neo4j.kernel.configuration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.factory.Migrator;
import org.neo4j.helpers.Pair;


public class AnnotatedFieldHarvester {

	/**
	 * Find all static fields of a given type, annotated with some given
	 * annotation.
	 * @param settingsClass
	 * @param type
	 * @param annotation
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> Iterable<Pair<Field, T>> findStatic(Class<?> clazz,
			Class<T> type, Class annotation) {
		List<Pair<Field, T>> found = new ArrayList<Pair<Field, T>>();
		for( Field field : clazz.getFields() )
        {
            try
            {
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
	 * @param settingsClass
	 * @param type
	 */
	public <T> Iterable<Pair<Field, T>> findStatic(Class<?> clazz,
			Class<T> type) {
		return findStatic(clazz, type, null);
	}

}
