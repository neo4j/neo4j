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
