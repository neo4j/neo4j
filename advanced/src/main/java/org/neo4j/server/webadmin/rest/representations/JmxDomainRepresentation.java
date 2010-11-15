/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.rest.representations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import org.neo4j.server.rest.domain.Representation;


public class JmxDomainRepresentation implements Representation
{

    protected ArrayList<JmxMBeanRepresentation> beans = new ArrayList<JmxMBeanRepresentation>();
    protected String domainName;

    public JmxDomainRepresentation( String name )
    {
        this.domainName = name;
    }

    public void addBean( ObjectName bean )
    {
        beans.add( new JmxMBeanRepresentation( bean ) );
    }

    public Object serialize()
    {
        Map<String, Object> data = new HashMap<String, Object>();

        data.put( "domain", this.domainName );

        ArrayList<Object> serialBeans = new ArrayList<Object>();
        for ( JmxMBeanRepresentation bean : beans )
        {
            serialBeans.add( bean.serialize() );
        }
        data.put( "beans", serialBeans );

        return data;
    }

}
