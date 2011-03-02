
define ->
  class FoldoutWatcher
    
    init : ->
      $("a.foldout_trigger").live "click", (ev) ->
        ev.preventDefault();
        $(ev.target).closest(".foldout").toggleClass "visible"
