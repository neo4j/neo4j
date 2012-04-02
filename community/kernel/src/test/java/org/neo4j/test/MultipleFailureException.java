package org.neo4j.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Snipped from jUnits MultipleFailureException
 */
public class MultipleFailureException extends Exception {
    private static final long serialVersionUID= 1L;
    
    private final List<Throwable> fErrors;

    public MultipleFailureException(List<Throwable> errors) {
        fErrors= new ArrayList<Throwable>(errors);
    }

    public List<Throwable> getFailures() {
        return Collections.unmodifiableList(fErrors);
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder(
                String.format("There were %d errors:", fErrors.size()));
        for (Throwable e : fErrors) {
            sb.append(String.format("\n  %s(%s)", e.getClass().getName(), e.getMessage()));
        }
        return sb.toString();
    }

}
