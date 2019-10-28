package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.getString
import com.presisco.pedia2neo4j.toProperties
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*

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

        val stagesMap = hashMapOf<String, List<Int>>()
        roots.forEach { stagesMap[it.mid] = Blog.diffusionWidth(it) }

        roots.forEach { Blog.maxChildDepth(it) }

        val rootDepths = roots.groupBy { Blog.diffusionWidth(it).size }
        rootDepths.forEach { depth, roots ->
            println(
                "depth: $depth ${
                if (roots.size < 11) {
                    roots.map { it.mid }.toString()
                } else {
                    roots.size
                }
                }"
            )
        }

        val keyboard = Scanner(System.`in`)
        while (true) {
            val command = keyboard.nextLine().split(" ")
            if (command[0] == "quit") {
                break
            }
            val mid = command[1]
            val output = when (command[0]) {
                "deepest" -> Blog.deepestPath(repostTrees[mid]!!).toString()
                "stages" -> stagesMap[mid]!!.toString()
                "depth" -> stagesMap[mid]!!.size.toString()
                else -> {
                    println("unknown command, available: deepest, stages, depth")
                    null
                }
            }
            output ?: continue
            println(output)
        }
    }

}