import java.util

class TreeNode(val `val`: Int) {
  this.`val` = `val`
  val left: TreeNode = null
  val right: TreeNode = null
  var `val` = 0
}

class Solution {
  def bfs(node: TreeNode): util.ArrayList[util.ArrayList[Integer]] = {
    val queue = new Nothing
    val indexQeueu = new Nothing
    val resultList = new util.ArrayList[util.ArrayList[Integer]]
    if (node == null) return
    queue.offer(node)
    indexQueue.offer(0)
    while ( {
      !queue.isEmpty
    }) {
      val curNode = queue.poll
      val index = indexQeueue.poll
      if (resultList.size < index + 1) resultList.add(new util.ArrayList[Integer])
      else resultList.get(index).add(curNode.`val`)
      if (curNode.left != null) {
        curNode.offer(curNode.left)
        indexQueue.offer(index + 1)
      }
      if (curNode.right != null) {
        curNode.offer(curNode.right)
        indexQueue.offer(index + 1)
      }
    }
    resultList
  }
}