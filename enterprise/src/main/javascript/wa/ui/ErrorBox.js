/**
 * Module for displaying errors.
 */
wa.ui.ErrorBox = function() {

    var currentErrors = {};

    return {
        /**
         * Display an error message until timout time passes.
         * 
         * @param error
         *            is the error string
         * @param timeout
         *            is the time in milliseconds to show the error, default is
         *            5000
         */
        showError : function(error, timeout) {
            var timeout = timeout || 5000;

            if (typeof (currentErrors[error]) !== "undefined")
            {
                var errObj = currentErrors[error];
                clearTimeout(errObj.timeout);

                $('.mor_error_count', errObj.elem).html(
                        "(" + (++errObj.count) + ")");
                $('.mor_error_count', errObj.elem).show();

                errObj.timeout = setTimeout((function(error) {
                    return function() {
                        wa.ui.ErrorBox.hideError(error);
                    };
                })(error), timeout);

            } else
            {
                currentErrors[error] = {
                    msg : error,
                    count : 1,
                    elem : $("<li>"
                            + error
                            + "<span class='mor_error_count' style='display:none;'>(1)</span></li>"),
                    timeout : setTimeout((function(error) {
                        return function() {
                            wa.ui.ErrorBox.hideError(error);
                        };
                    })(error), timeout)
                };

                $("#mor_errors").append(currentErrors[error].elem);
            }
        },

        hideError : function(error) {
            if (typeof (currentErrors[error]) !== "undefined")
            {
                currentErrors[error].elem.remove();
                delete (currentErrors[error]);
            }
        }
    };

}();