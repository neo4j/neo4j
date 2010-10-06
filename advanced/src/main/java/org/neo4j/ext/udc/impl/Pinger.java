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

package org.neo4j.ext.udc.impl;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

public class Pinger {
    
    private String address;
    private Map<String, String> usageDataMap;

    public Pinger(String address, Map<String, String> usageDataMap) {
        this.address = address;
        this.usageDataMap = usageDataMap;
    }


    public void ping() throws IOException {
        StringBuffer uri = new StringBuffer("http://" + address + "/" + "?");

        Iterator<String> keyIt = usageDataMap.keySet().iterator();
        while (keyIt.hasNext()) {
            String key = keyIt.next();
            uri.append(key);
            uri.append("=");
            uri.append(usageDataMap.get(key));
            if (keyIt.hasNext()) uri.append("+");
        }

        URL url = new URL(uri.toString());
        URLConnection con = url.openConnection();

        con.setDoInput(true);
        con.setDoOutput(false);
        con.setUseCaches(false);
        con.connect();

        con.getInputStream();
    }

}