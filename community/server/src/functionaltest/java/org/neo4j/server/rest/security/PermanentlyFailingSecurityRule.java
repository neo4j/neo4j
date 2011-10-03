package org.neo4j.server.rest.security;

import javax.servlet.http.HttpServletRequest;

public class PermanentlyFailingSecurityRule implements SecurityRule {
    
    public static final String REALM = "WallyWorld"; // as per RFC2617 :-);

    @Override
    public boolean isAuthorized( HttpServletRequest request )
    {
        return false; // always fails
    }

    @Override
    public String forUriPath()
    {
        return SecurityRule.DEFAULT_DATABASE_PATH;
    }

    @Override
    public String forRealm()
    {
        return REALM;
    }
}
