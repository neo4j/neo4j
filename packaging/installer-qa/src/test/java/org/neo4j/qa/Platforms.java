/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.qa;

import org.apache.commons.lang.StringUtils;

public class Platforms {

    /**
     *  Windows with windows installers
     */
    public static final String WINDOWS = "windows";
    
    /**
     *  Ubuntu with debian packages
     */
    public static final String UBUNTU_DEB = "ubuntu-deb";
    
    /**
     *  Ubuntu with tarball packages
     */
    public static final String UBUNTU_TAR_GZ = "ubuntu-tar-gz";

    /**
     * Comma separated list of all available platforms.
     */
    public static final String ALL = StringUtils.join(new String[]{
            WINDOWS,
            UBUNTU_DEB,
            UBUNTU_TAR_GZ},",");

    public static String[] selectedPlatforms() {
        return System.getProperty("test-platforms", Platforms.ALL).split(",");
    }
    
}
