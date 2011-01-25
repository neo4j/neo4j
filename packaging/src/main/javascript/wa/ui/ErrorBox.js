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
                var errObj = currentErrors[error],
                    errCount = errObj.count < 5 ? (++errObj.count) : "5+";
                clearTimeout(errObj.timeout);

                $('.mor_error_count', errObj.elem).html("(" + errCount + ")");
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