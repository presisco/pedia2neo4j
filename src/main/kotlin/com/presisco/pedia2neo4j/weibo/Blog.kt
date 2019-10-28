package com.presisco.pedia2neo4j.weibo

data class Blog(
    val mid: String,
    val childs: MutableSet<Blog> = hashSetOf(),
    var childDepth: Int = 0,
    var valid: Boolean = false
) {
    override fun hashCode(): Int {
        return mid.hashCode()
    }

    fun addChild(blog: Blog) = childs.add(blog)

    companion object {
        fun diffusionWidth(blog: Blog, depth: Int = 0, stages: MutableList<Int> = arrayListOf(0)): List<Int> {
            if (depth == stages.size) {
                stages.add(0)
            }
            stages[depth]++
            blog.childs.forEach { diffusionWidth(it, depth + 1, stages) }
            return stages
        }

        fun maxChildDepth(blog: Blog, level: Int = 0): Int {
            if (blog.childs.isEmpty()) {
                return level
            }
            var maxDepth = -1
            blog.childs.forEach {
                val childDepth = maxChildDepth(it, level + 1)
                if (childDepth > maxDepth) {
                    maxDepth = childDepth
                }
            }
            blog.childDepth = maxDepth
            return maxDepth
        }

        fun deepestPath(blog: Blog, path: MutableList<String> = arrayListOf()): List<String> {
            path.add(blog.mid)
            if (blog.childs.isEmpty()) {
                return path
            }
            val deepestChild = blog.childs.maxBy { it.childDepth }!!
            deepestPath(deepestChild, path)
            return path
        }
    }
}