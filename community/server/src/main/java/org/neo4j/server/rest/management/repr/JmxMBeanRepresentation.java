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
package org.neo4j.server.rest.management.repr;

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.URLEncoder;
import java.util.Arrays;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.ObjectRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.ValueRepresentation;

public class JmxMBeanRepresentation extends ObjectRepresentation
{

    protected ObjectName beanName;
    protected MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();

    public JmxMBeanRepresentation( ObjectName beanInstance )
    {
        super( "jmxBean" );
        this.beanName = beanInstance;
    }

    @Mapping( "name" )
    public ValueRepresentation getName()
    {
        return ValueRepresentation.string( beanName.getCanonicalName() );
    }

    @Mapping( "url" )
    public ValueRepresentation getUrl()
    {
        try
        {
            String value = URLEncoder.encode( beanName.toString(), "UTF-8" )
                    .replace( "%3A", "/" );
            return ValueRepresentation.string( value );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "Could not encode string as UTF-8", e );
        }
    }

    @Mapping( "description" )
    public ValueRepresentation getDescription() throws IntrospectionException, InstanceNotFoundException,
            ReflectionException
    {
        MBeanInfo beanInfo = jmxServer.getMBeanInfo( beanName );
        return ValueRepresentation.string( beanInfo.getDescription() );
    }

    @Mapping( "attributes" )
    public ListRepresentation getAttributes() throws IntrospectionException, InstanceNotFoundException,
            ReflectionException
    {
        MBeanInfo beanInfo = jmxServer.getMBeanInfo( beanName );

        return new ListRepresentation( "jmxAttribute", new IterableWrapper<Representation, MBeanAttributeInfo>(
                Arrays.asList( beanInfo.getAttributes() ) )
        {
            @Override
            protected Representation underlyingObjectToObject( MBeanAttributeInfo attrInfo )
            {
                return new JmxAttributeRepresentation( beanName, attrInfo );
            }
        } );
    }
}
