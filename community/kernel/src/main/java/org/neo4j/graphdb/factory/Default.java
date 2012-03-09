package org.neo4j.graphdb.factory;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * TODO
 */
@Retention( RetentionPolicy.RUNTIME )
public @interface Default
{
    String value();
}
