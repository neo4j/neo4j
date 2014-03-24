/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

'use strict';

function CypherConsole(config, ready) {

    var $IFRAME = $('<iframe/>').attr('id', 'console').addClass('cypherdoc-console');
    var RESIZE_OUT_ICON = 'fa-expand';
    var RESIZE_IN_ICON = 'fa-compress';
    var $RESIZE_BUTTON = $('<a class="btn btn-xs btn-primary resize-toggle"><i class="fa ' + RESIZE_OUT_ICON + '"></i></a>');
    var $RESIZE_VERTICAL_BUTTON = $('<span class="resize-vertical-handle ui-resizable-handle ui-resizable-s"><span></span></span>');
    var $PLAY_BUTTON = $('<a class="run-query btn btn-success" data-toggle="tooltip" title="Execute in the console." href="#"><i class="fa fa-play"></i></a>');
    var $EDIT_BUTTON = $('<a class="edit-query btn btn-default" data-toggle="tooltip" title="Copy to the console." href="#"><i class="fa fa-clipboard"></i></a>');
    var $BUTTON_GROUP = $('<div class="btn-group btn-group-xs btn-group-vertical"/>');

    var $resizeOverlay = $('<div id="resize-overlay"/>');
    var $header = $('#header');

    var consolr;
    var consoleClass = 'consoleClass' in config ? config.consoleClass : 'console';
    var contentId = 'contentId' in config ? config.contentId : 'content';
    var contentMoveSelector = 'contentMoveSelector' in config ? config.contentMoveSelector : 'div.navbar';
    var consoleUrl = config.url;
    var expandHeightCorrection = 'expandHeightCorrection' in config ? config.expandHeightCorrection : 0;
    var expandEventHandlers = {
        'beforeExpand' : getFunctionFromConfig( 'beforeExpand' ),
        'afterExpand' : getFunctionFromConfig( 'afterExpand' ),
        'beforeUnexpand' : getFunctionFromConfig( 'beforeUnexpand' ),
        'afterUnexpand' : getFunctionFromConfig( 'afterUnexpand' )
    };
    
    function getFunctionFromConfig( name )
    {
      return name in config ? config[name] : function(){};
    }

    createConsole(ready, consoleClass, contentId);

    function createConsole(ready, elementClass, contentId) {
        var $element = $('p.' + elementClass).first();
        //no console defined
        if ($element.length !== 1) {
            $element = $('<p/>').addClass(elementClass);
            $element.addClass("hidden");
//            console.log("createConsole", $element);
            $('#' + contentId).append($element);
        }
        $element.each(function () {
            var $context = $(this);
            addConsole($context, ready)
        });
        addPlayButtons();
    }

    function addConsole($context, ready) {
        var url = getUrl('none', 'none', '\n\nUse the play/copy buttons to run the queries!');
        var $iframe = $IFRAME.clone().attr('src', url);
        $iframe.load(function () {
            consolr = new Consolr($iframe[0].contentWindow);
            if (ready) {
                ready(consolr);
            }
        });
        $context.empty();
        $context.append($iframe);
        var $contentMoveSelector = $(contentMoveSelector).first();
        $context.css('background', 'none');
        var latestResizeAmount = 0;
        var $verticalResizeButton = $RESIZE_VERTICAL_BUTTON.clone().appendTo($context).mousedown(function (event) {
            event.preventDefault();
        });
        $context.resizable({'handles': {'s': $verticalResizeButton}, 'minHeight': 80, 'start': function () {
                $resizeOverlay.appendTo($context);
            }, 'stop': function (event, ui) {
                $resizeOverlay.detach();
                latestResizeAmount = ui.size.height - ui.originalSize.height;
                $context.css('width', 'auto');
            }, 'resize': function (event, ui) {
                if (!$resizeIcon.hasClass(RESIZE_OUT_ICON)) {
                    $contentMoveSelector.css('top', ui.size.height + expandHeightCorrection);
                }
            }}
        );

        var contextHeight = 0;
        var $resizeButton = $RESIZE_BUTTON.clone().appendTo($context).click(function () {
            if ($resizeIcon.hasClass(RESIZE_OUT_ICON)) {
                expandEventHandlers.beforeExpand();
                $resizeIcon.removeClass(RESIZE_OUT_ICON).addClass(RESIZE_IN_ICON);
                $context.addClass('fixed-console');
                $contentMoveSelector.css('top', $context.height() + expandHeightCorrection);
                expandEventHandlers.afterExpand();
            } else {
                expandEventHandlers.beforeUnexpand();
                $resizeIcon.removeClass(RESIZE_IN_ICON).addClass(RESIZE_OUT_ICON);
                $context.removeClass('fixed-console');
                $contentMoveSelector.css('top', 100); // TODO
                expandEventHandlers.afterUnexpand();
            }
        });

        var $resizeIcon = $('i', $resizeButton);
    }

    function addPlayButtons() {
        $('div.query-wrapper').each(function(){          
          $(this).css("margin-left", "32px").parent().each(function(){
            $BUTTON_GROUP.clone().appendTo(this).append($PLAY_BUTTON.clone().click(function (event) {
                event.preventDefault();
                consolr.query([ getQueryFromButton(this) ]);
            })).append($EDIT_BUTTON.clone().click(function (event) {
                event.preventDefault();
                consolr.input(getQueryFromButton(this));
            }));
          });
        });        
    }

    function getQueryFromButton(button) {
        return $(button).parent().prevAll('div.query-wrapper').first().data('query');
    }

    function getUrl(database, command, message, session) {
        var url = consoleUrl;

        if (session !== undefined) {
            url += ';jsessionid=' + session;
        }
        url += '?';
        if (database !== undefined) {
            url += 'init=' + encodeURIComponent(database);
        }
        if (command !== undefined) {
            url += '&query=' + encodeURIComponent(command);
        }
        if (message !== undefined) {
            url += '&message=' + encodeURIComponent(message);
        }
        if (window.neo4jVersion != undefined) {
            url += '&version=' + encodeURIComponent(neo4jVersion);
        }
        return url + '&no_root=true';
    }
}

function Consolr(consoleWindow) {
    window.addEventListener('message', receiver, false);
    var receivers = [];

    function init(params, success, error, data) {
        var index = 0;
        if (success || error) {
            receivers.push(new ResultReceiver(success, error));
            index = receivers.length;
        }
        consoleWindow.postMessage(JSON.stringify({
            'action': 'init',
            'data': params,
            'call_id': index
        }), "*");
    }

    function query(queries, success, error) {
        var index = 0;
        if (success || error) {
            receivers.push(new ResultReceiver(success, error, queries.length));
            index = receivers.length;
        }
        var message = JSON.stringify({
            'action': 'query',
            'data': queries,
            'call_id': index
        });
        consoleWindow.postMessage(message, '*');
    }

    function input(query) {
        consoleWindow.postMessage(JSON.stringify({
            'action': 'input',
            'data': [ query ]
        }), '*');
    }

    function receiver(event) {
        var origin = event.origin;
        if (typeof origin !== 'string') {
            return;
        }
        if (origin.indexOf('neo4j') === -1 && origin.indexOf('localhost') === -1) {
            return;
        }
        var result = JSON.parse(event.data);
        if ('call_id' in result) {
            var rr = receivers[result.call_id - 1];
            rr(result);
        }
    }

    function ResultReceiver(successFunc, errorFunc, numberOfResults) {
        var expectedResults = numberOfResults || 1;

        function call(result) {
            if (expectedResults === 0) {
                console.log('Unexpected result', result);
                return;
            }
            expectedResults--;
            var resultNo = numberOfResults - expectedResults - 1;
            return result.error ? errorFunc(result, resultNo) : successFunc(result, resultNo);
        }

        return call;
    }

    return {
        'init': init,
        'query': query,
        'input': input
    };
}