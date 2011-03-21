/*
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


wa.FormValidator = (function($){
	
	/**
	 * Built-in validators.
	 */
	var validators = {
		'not_empty' : function(value) {
			return typeof(value) === "string" && value.length > 0;
		}
	};
	
	return {
        
        validateField : function( field, validator, errorMessage ) {
            
            var field = $(field);
            validator = typeof(validator) === "function" ? validator : validators[validator];
            
            if( ! validator(field.val(), field) ) {
                field.addClass("error");
                return false;
            } else {
                field.removeClass("error");
                return true;
            }
        },
        
        validateFields : function(def) {
            var success = true;
            
            for( var i = 0, l = def.length; i<l; i++) {
                if(!wa.FormValidator.validateField(def[i].field, def[i].validator, def[i].errorMessage)) {
                    success = false;
                }
            }
            
            return success;
            
        }
            
    };
	
})(jQuery);