package org.neo4j.kernel.impl.management;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.management.MBeanOperationInfo;

@Target( { ElementType.METHOD, ElementType.TYPE, ElementType.FIELD } )
@Retention( RetentionPolicy.RUNTIME )
public @interface Description
{
    // TODO: refactor for localization
    String value();

    int impact() default MBeanOperationInfo.UNKNOWN;
}
