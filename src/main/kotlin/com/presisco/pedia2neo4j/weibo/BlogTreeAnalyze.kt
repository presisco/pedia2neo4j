package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.getString
import com.presisco.pedia2neo4j.toProperties
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

object BlogTreeAnalyze {

    val db = MapJdbcClient(
        HikariDataSource(
            HikariConfig(
                mapOf(
                    "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
                    "dataSource.url" to "jdbc:sqlite:scrappy_weibo.db",
                    "maximumPoolSize" to "1"
                ).toProperties()
            )
        )
    )

    fun setRootPointer(rootMid: String, blog: Blog, updateList: MutableList<Map<String, String>>) {
        updateList.addAll(blog.childs.map { mapOf("mid" to it.mid, "root_id" to rootMid) })
        blog.childs.forEach { setRootPointer(rootMid, it, updateList) }
    }

    @JvmStatic
    fun main(vararg args: String) {
        val iterator = db.selectIterator("select mid, repost_id from blog")
        val repostTrees = hashMapOf<String, Blog>()
        val roots = hashMapOf<Blog, Int>()
        while (iterator.hasNext()) {
            val row = iterator.next()

            val mid = row.getString("mid")
            if (!repostTrees.containsKey(mid)) {
                repostTrees[mid] = Blog(mid)
                repostTrees[mid]!!.valid = true
            }

            if (row["repost_id"] != null) {
                val repostId = row.getString("repost_id")
                if (!repostTrees.containsKey(repostId)) {
                    repostTrees[repostId] = Blog(repostId)
                    roots[repostTrees[repostId]!!] = 0
                } else {
                    roots.remove(repostTrees[mid]!!)
                }
                repostTrees[repostId]!!.addChild(repostTrees[mid]!!)
            } else {
                roots[repostTrees[mid]!!] = 0
            }
        }

        println("unique blogs: ${repostTrees.keys.size}")
        println("trees: ${roots.size}")

        val rootUpdateList = arrayListOf<Map<String, *>>()
        val blogUpdateList = arrayListOf<Map<String, String>>()
        for (blog in roots.keys) {
            roots[blog] = Blog.maxDepth(blog)
            setRootPointer(blog.mid, blog, blogUpdateList)
            rootUpdateList.add(
                mapOf(
                    "mid" to blog.mid,
                    "depth" to roots[blog]
                )
            )
        }
        db.executeBatch(
            { "update root set depth = ? where mid = ?" },
            rootUpdateList,
            db.buildTypeMapSubset("root", rootUpdateList),
            listOf("depth", "mid")
        )
        db.executeBatch(
            { "update blog set root_id = ? where mid = ?" },
            blogUpdateList,
            db.buildTypeMapSubset("blog", blogUpdateList),
            listOf("root_id", "mid")
        )

        roots.values.groupBy { it }.forEach { depth, counts ->
            println("depth: $depth has ${counts.size} trees")
        }
    }

}