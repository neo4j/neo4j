package org.neo4j.server.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

@Path("/")
public class WelcomePage {
    
    public static Logger log = Logger.getLogger(WelcomePage.class);
    
    @GET
    @Path("welcome.html")
    @Produces(MediaType.TEXT_HTML)
    public Response welcome() {
        return Response.ok().type(MediaType.TEXT_HTML).entity(loadHtml()).build();
    }

    private String loadHtml() {
        return readAsString(ClassLoader.getSystemResourceAsStream("welcome.html"));
    }

    private static String readAsString(InputStream input) {
        final char[] buffer = new char[0x10000];
        StringBuilder out = new StringBuilder();
        Reader reader = null;
        try {
            reader = new InputStreamReader(input);
            int read;
            do {
                read = reader.read(buffer, 0, buffer.length);
                if (read > 0) {
                    out.append(buffer, 0, read);
                }
            } while (read >= 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.info(e.getMessage());
                }
            }
        }
        return out.toString();
    }
}
