
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