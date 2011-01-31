/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.release.it.std.exec;

import java.net.URL;

/**
 * The specification for how to provision an artifact.
 *
 */
public class ArtifactProvision {
    private URL sourceURL;
    private String destinationFilename;

    public ArtifactProvision(URL sourceURL, String destinationFilename) {
        this.sourceURL = sourceURL;
        this.destinationFilename = destinationFilename;
    }

    public URL getSourceURL() {
        return sourceURL;
    }

    public String getDestinationFilename() {
        return destinationFilename;
    }

    @Override
    public String toString() {
        return "ArtifactProvision{" +
                "sourceURL=" + sourceURL +
                ", destinationFilename='" + destinationFilename + '\'' +
                '}';
    }
}
