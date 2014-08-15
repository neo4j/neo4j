neo.utils.cloneArray = (original) ->
  clone = new Array(original.length)
  clone[idx] = node for node, idx in original
  clone