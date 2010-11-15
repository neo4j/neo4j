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

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.neo4j.server.rest.domain.Representation;

public class JmxMBeanRepresentation implements Representation {

    protected ObjectName beanName;
    protected MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();

    public JmxMBeanRepresentation(ObjectName beanInstance) {
        this.beanName = beanInstance;
    }

    public Object serialize() {

        Map<String, Object> data = new HashMap<String, Object>();

        data.put("name", beanName.toString());
        data.put("url", "");

        try {
            MBeanInfo beanInfo = jmxServer.getMBeanInfo(beanName);

            data.put("description", beanInfo.getDescription());
            data.put("url", mBean2Url(beanName));

            ArrayList<Object> attributes = new ArrayList<Object>();
            for (MBeanAttributeInfo attrInfo : beanInfo.getAttributes()) {
                attributes.add((new JmxAttributeRepresentation(beanName, attrInfo)).serialize());
            }

            data.put("attributes", attributes);

        } catch (IntrospectionException e) {
            e.printStackTrace();
        } catch (InstanceNotFoundException e) {
            e.printStackTrace();
        } catch (ReflectionException e) {
            e.printStackTrace();
        }

        return data;
    }

    private static String mBean2Url(ObjectName obj) {
        try {
            return URLEncoder.encode(obj.toString(), "UTF-8").replace("%3A", "/");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Could not encode string as UTF-8", e);
        }
    }
}
