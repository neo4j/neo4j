/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.metrics.docs;

import java.lang.reflect.Field;

import org.neo4j.kernel.impl.annotations.Documented;

public class MetricsAsciiDocGenerator
{
    private static final String NEW_LINE = System.lineSeparator();

    public void generateDocsFor( String metricsResource, StringBuilder builder )
    {
        Class<?> clazz;
        try
        {
            clazz = Class.forName( metricsResource );
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException( "Couldn't load metrics class: ", e );
        }

        {
            Documented documented =  clazz.getAnnotation( Documented.class );
            if ( documented == null )
            {
                throw new IllegalStateException( "Missing Documented annotation on the class: " + metricsResource );
            }
            builder.append( documented.value() ).append( NEW_LINE ).append( NEW_LINE );
        }

        Field[] fields = clazz.getDeclaredFields();
        if ( existsDocumentedFields( fields ) )
        {

            builder.append( "[options=\"header\",cols=\"<1m,<4\"]" ).append( NEW_LINE );
            builder.append( "|===" ).append( NEW_LINE );
            builder.append( "|Name |Description" ).append( NEW_LINE );

            for ( Field field : fields )
            {
                documentField( builder, clazz, field );
            }

            builder.append( "|===" ).append( NEW_LINE );
            builder.append( NEW_LINE );
        }
    }

    private boolean existsDocumentedFields( Field[] fields )
    {
        for ( Field field : fields )
        {
            if ( field.isAnnotationPresent( Documented.class ) )
            {
                return true;
            }
        }
        return false;
    }

    private void documentField( StringBuilder builder, Class clazz, Field field )
    {
        Documented documented = field.getAnnotation( Documented.class );
        if ( documented != null )
        {
            String name = getStaticFieldValue( clazz, field );
            builder.append( "|" ).append( name )
                    .append( "|" ).append( documented.value() )
                    .append( NEW_LINE );
        }
    }

    private <T> T getStaticFieldValue( Class clazz, Field field )
    {
        try
        {
            //noinspection unchecked
            return (T) field.get( null );
        }
        catch ( IllegalAccessException e )
        {
            throw new IllegalStateException( "Cannot fetch value of field " + field + " in " + clazz, e );
        }
    }

}
