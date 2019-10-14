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

    @JvmStatic
    fun main(vararg args: String) {
        val iterator = db.selectIterator("select mid, repost_id from blog")
        val repostTrees = hashMapOf<String, Blog>()
        val roots = hashSetOf<Blog>()
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
                    roots.add(repostTrees[repostId]!!)
                } else {
                    roots.remove(repostTrees[mid]!!)
                }
                repostTrees[repostId]!!.addChild(repostTrees[mid]!!)
            } else {
                roots.add(repostTrees[mid]!!)
            }
        }

        println("unique blogs: ${repostTrees.keys.size}")
        println("trees: ${roots.size}")
        val diffusions = hashMapOf<String, List<Int>>()
        roots.forEach {
            diffusions[it.mid] = Blog.diffusionWidth(it)
        }

        val depths = diffusions.mapValues { it.value.size }
        val maxDepth = depths.maxBy { it.value }!!.value
        println("max depth blogs: ${diffusions.filterValues { it.size == maxDepth }.keys}")

        val depthStats = diffusions.values.map { it.size }.groupBy { it }.mapValues { it.value.size }
        depthStats.forEach { depth, count -> println("depth: $depth has count: $count") }

        val mostDepth = depthStats.maxBy { it.value }
        println("most depth: $mostDepth")
    }

}