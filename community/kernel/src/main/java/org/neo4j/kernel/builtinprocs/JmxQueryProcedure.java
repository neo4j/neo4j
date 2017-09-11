/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.builtinprocs;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.QualifiedName;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.Pair.pair;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class JmxQueryProcedure extends CallableProcedure.BasicProcedure
{
    private final MBeanServer jmxServer;

    public JmxQueryProcedure( QualifiedName name, MBeanServer jmxServer )
    {
        super( procedureSignature( name )
                .in( "query", Neo4jTypes.NTString )
                .out( "name", Neo4jTypes.NTString )
                .out( "description", Neo4jTypes.NTString )
                .out( "attributes", Neo4jTypes.NTMap )
                .description( "Query JMX management data by domain and name. For instance, \"org.neo4j:*\"" )
                .build() );
        this.jmxServer = jmxServer;
    }

    @Override
    public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        String query = input[0].toString();
        try
        {
            // Find all beans that match the query name pattern
            Iterator<ObjectName> names = jmxServer.queryNames( new ObjectName( query ), null ).iterator();

            // Then convert them to a Neo4j type system representation
            return RawIterator.from( () ->
            {
                if ( !names.hasNext() )
                {
                    return null;
                }

                ObjectName name = names.next();
                try
                {
                    MBeanInfo beanInfo = jmxServer.getMBeanInfo( name );
                    return new Object[]{
                            name.getCanonicalName(),
                            beanInfo.getDescription(),
                            toNeo4jValue( name, beanInfo.getAttributes() ) };
                }
                catch ( JMException e )
                {
                    throw new ProcedureException( Status.General.UnknownError,
                            e, "JMX error while accessing `%s`, please report this. Message was: %s",
                            name, e.getMessage() );
                }
            });
        }
        catch ( MalformedObjectNameException e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                  "'%s' is an invalid JMX name pattern. Valid queries should use" +
                  "the syntax outlined in the javax.management.ObjectName API documentation." +
                  "For instance, try 'org.neo4j:*' to find all JMX beans of the 'org.neo4j' " +
                  "domain, or '*:*' to find every JMX bean.", query );
        }
    }

    private Map<String,Object> toNeo4jValue( ObjectName name, MBeanAttributeInfo[] attributes )
            throws JMException
    {
        HashMap<String,Object> out = new HashMap<>();
        for ( MBeanAttributeInfo attribute : attributes )
        {
            if ( attribute.isReadable() )
            {
                out.put( attribute.getName(), toNeo4jValue( name, attribute ) );
            }
        }
        return out;
    }

    private Map<String,Object> toNeo4jValue( ObjectName name, MBeanAttributeInfo attribute )
            throws JMException
    {
        Object value;
        try
        {
            value = toNeo4jValue( jmxServer.getAttribute( name, attribute.getName() ) );
        }
        catch ( RuntimeMBeanException e )
        {
            if ( e.getCause() != null && e.getCause() instanceof UnsupportedOperationException )
            {
                // We include the name and description of this attribute still - but the value of it is
                // unknown. We do this rather than rethrow the exception, because several MBeans built into
                // the JVM will throw exception on attribute access depending on their runtime state, even
                // if the attribute is marked as readable. Notably the GC beans do this.
                value = null;
            }
            else
            {
                throw e;
            }
        }
        return map(
            "description", attribute.getDescription(),
            "value", value
        );
    }

    private Object toNeo4jValue( Object attributeValue )
    {
        // These branches as per {@link javax.management.openmbean.OpenType#ALLOWED_CLASSNAMES_LIST}
        if ( isSimpleType( attributeValue ) )
        {
            return attributeValue;
        }
        else if ( attributeValue.getClass().isArray() )
        {
            if ( isSimpleType( attributeValue.getClass().getComponentType() ) )
            {
                return attributeValue;
            }
            else
            {
                return toNeo4jValue( (Object[]) attributeValue );
            }
        }
        else if ( attributeValue instanceof CompositeData )
        {
            return toNeo4jValue( (CompositeData) attributeValue );
        }
        else if ( attributeValue instanceof ObjectName )
        {
            return ((ObjectName) attributeValue).getCanonicalName();
        }
        else if ( attributeValue instanceof TabularData )
        {
            return toNeo4jValue( (Map<?,?>) attributeValue );
        }
        else if ( attributeValue instanceof Date )
        {
            return ((Date) attributeValue).getTime();
        }
        else
        {
            // Don't convert objects that are not OpenType values
            return null;
        }
    }

    private Map<String,Object> toNeo4jValue( Map<?,?> attributeValue )
    {
        // Build a new map with the same keys, but each value passed
        // through `toNeo4jValue`
        return attributeValue.entrySet().stream()
                .map( e -> pair( e.getKey().toString(), toNeo4jValue( e.getValue() ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );
    }

    private List<Object> toNeo4jValue( Object[] array )
    {
        return Arrays.stream(array).map( this::toNeo4jValue ).collect( Collectors.toList() );
    }

    private Map<String, Object> toNeo4jValue( CompositeData composite )
    {
        HashMap<String,Object> properties = new HashMap<>();
        for ( String key : composite.getCompositeType().keySet() )
        {
            properties.put( key, toNeo4jValue(composite.get( key )) );
        }

        return map(
            "description", composite.getCompositeType().getDescription(),
            "properties", properties
        );
    }

    private boolean isSimpleType( Object value )
    {
        return value == null || isSimpleType( value.getClass() );
    }

    private boolean isSimpleType( Class<?> cls )
    {
        return String.class.isAssignableFrom( cls ) ||
               Number.class.isAssignableFrom( cls ) ||
               Boolean.class.isAssignableFrom( cls ) ||
               cls.isPrimitive();
    }
}
