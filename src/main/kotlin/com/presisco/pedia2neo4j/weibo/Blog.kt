package com.presisco.pedia2neo4j.weibo

data class Blog(
    val mid: String,
    val childs: MutableSet<Blog> = hashSetOf(),
    var uid: String = "",
    var time: String = "",
    var maxDepth: Int = 0,
    var like: Int = 0,
    var comment: Int = 0,
    var repost: Int = 0,
    var scrapTime: String = "0000-00-00 00:00:00",
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

        fun maxDepth(blog: Blog, level: Int = 0): Int {
            if (blog.childs.isEmpty()) {
                blog.maxDepth = level
                return level
            }
            blog.childs.forEach { maxDepth(it, level + 1) }
            val deepest = blog.childs.maxBy { it.maxDepth }
            blog.maxDepth = deepest!!.maxDepth
            return blog.maxDepth
        }
    }
}