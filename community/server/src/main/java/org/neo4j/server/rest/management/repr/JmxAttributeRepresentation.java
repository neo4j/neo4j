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

import java.lang.management.ManagementFactory;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.neo4j.server.rest.repr.ObjectRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationDispatcher;
import org.neo4j.server.rest.repr.ValueRepresentation;

public class JmxAttributeRepresentation extends ObjectRepresentation
{

    protected ObjectName objectName;
    protected MBeanAttributeInfo attrInfo;
    protected MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
    private static final RepresentationDispatcher REPRESENTATION_DISPATCHER = new JmxAttributeRepresentationDispatcher();

    public JmxAttributeRepresentation( ObjectName objectName, MBeanAttributeInfo attrInfo )
    {
        super( "jmxAttribute" );
        this.objectName = objectName;
        this.attrInfo = attrInfo;
    }

    @Mapping( "name" )
    public ValueRepresentation getName()
    {
        return ValueRepresentation.string( attrInfo.getName() );
    }

    @Mapping( "description" )
    public ValueRepresentation getDescription()
    {
        return ValueRepresentation.string( attrInfo.getDescription() );
    }

    @Mapping( "type" )
    public ValueRepresentation getType()
    {
        return ValueRepresentation.string( attrInfo.getType() );
    }

    @Mapping( "isReadable" )
    public ValueRepresentation isReadable()
    {
        return bool( attrInfo.isReadable() );
    }

    @Mapping( "isWriteable" )
    public ValueRepresentation isWriteable()
    {
        return bool( attrInfo.isWritable() );
    }

    @Mapping( "isIs" )
    public ValueRepresentation isIs()
    {
        return bool( attrInfo.isIs() );
    }

    private ValueRepresentation bool( Boolean value )
    {
        return ValueRepresentation.string( value ? "true" : "false " );
    }

    @Mapping( "value" )
    public Representation getValue()
    {
        try
        {
            Object value = jmxServer.getAttribute( objectName, attrInfo.getName() );
            return REPRESENTATION_DISPATCHER.dispatch( value, "" );
        }
        catch ( Exception e )
        {
            return ValueRepresentation.string( "N/A" );
        }
    }

}
