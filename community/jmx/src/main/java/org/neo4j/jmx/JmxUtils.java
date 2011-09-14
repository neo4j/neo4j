/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.jmx;

import org.neo4j.kernel.AbstractGraphDatabase;

import javax.management.*;
import java.lang.management.ManagementFactory;

import static java.lang.String.format;

public class JmxUtils {

    private static final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    public static ObjectName getObjectName(AbstractGraphDatabase database, String name) {
        ObjectName neoQuery = database.getSingleManagementBean(Kernel.class).getMBeanQuery();
        String instance = neoQuery.getKeyProperty("instance");
        String domain = neoQuery.getDomain();
        try {
            return new ObjectName(format("%s:instance=%s,name=%s", domain, instance, name));
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T getAttribute(ObjectName objectName, String attribute) {
        try {
            return (T) mbeanServer.getAttribute(objectName, attribute);
        } catch (MBeanException e) {
            throw new RuntimeException(e);
        } catch (AttributeNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstanceNotFoundException e) {
            throw new RuntimeException(e);
        } catch (ReflectionException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T invoke(ObjectName objectName, String attribute, Object[] params, String[] signatur) {
        try {
            return (T) mbeanServer.invoke(objectName, attribute, params, signatur);
        } catch (MBeanException e) {
            throw new RuntimeException(e);
        } catch (InstanceNotFoundException e) {
            throw new RuntimeException(e);
        } catch (ReflectionException e) {
            throw new RuntimeException(e);
        }
    }
}
