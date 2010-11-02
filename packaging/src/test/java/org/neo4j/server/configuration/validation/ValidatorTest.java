package org.neo4j.server.configuration.validation;

import static org.junit.Assert.*;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;


public class ValidatorTest {
    @Test
    public void shouldFailWhenRuleFails() {
       Validator v = new Validator( new ValidationRule() {

        public void validate(Configuration configuration) throws RuleFailedException {
            throw new RuleFailedException("dummy rule failed during unit test");
        }});
       
       assertFalse(v.validate(null));
    }
    
    @Test
    public void shouldFailWhenAtLeastOneRuleFails() {
       Validator v = new Validator( new ValidationRule() {

           public void validate(Configuration configuration) throws RuleFailedException {
               // do nothing
           }},
       new ValidationRule() {

           public void validate(Configuration configuration) throws RuleFailedException {
               throw new RuleFailedException("dummy rule failed during unit test");
           }},
       new ValidationRule() {

           public void validate(Configuration configuration) throws RuleFailedException {
               // do nothing
           }},
       new ValidationRule() {

           public void validate(Configuration configuration) throws RuleFailedException {
               // do nothing
           }});
       
       assertFalse(v.validate(null));
    }
    
    @Test
    public void shouldPassWhenAllRulesComplete() {
       Validator v = new Validator( new ValidationRule() {

           public void validate(Configuration configuration) throws RuleFailedException {
               // do nothing
           }},
       new ValidationRule() {

           public void validate(Configuration configuration) throws RuleFailedException {
               // do nothing
           }},
       new ValidationRule() {

           public void validate(Configuration configuration) throws RuleFailedException {
               // do nothing
           }},
       new ValidationRule() {

           public void validate(Configuration configuration) throws RuleFailedException {
               // do nothing
           }});
       
       assertTrue(v.validate(null));
    }
}
