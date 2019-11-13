package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.toProperties
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*

object BlogTreeAnalyze {

    val db = MapJdbcClient(
        HikariDataSource(
            HikariConfig(
                mapOf(
                    "dataSourceClassName" to "com.mysql.cj.jdbc.MysqlDataSource",
                    "dataSource.url" to "jdbc:mysql://localhost:3306/weibo?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC",
                    "dataSource.user" to "root",
                    "dataSource.password" to "experimental",
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

        val woods = Blog.buildWoods(iterator)
        val blogs = woods.first
        val roots = woods.second
        val rootDepths = hashMapOf<Blog, Int>()

        println("unique blogs: ${blogs.keys.size}")
        println("trees: ${roots.size}")

        val rootUpdateList = arrayListOf<Map<String, *>>()
        val blogUpdateList = arrayListOf<Map<String, String>>()
        for (blog in roots) {
            rootDepths[blog] = Blog.maxDepth(blog)
            blogUpdateList.add(
                mapOf(
                    "mid" to blog.mid, "root_id" to blog.mid
                )
            )
            setRootPointer(blog.mid, blog, blogUpdateList)
            rootUpdateList.add(
                mapOf(
                    "mid" to blog.mid,
                    "depth" to rootDepths[blog]
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

        val depthMap = rootDepths.entries.groupBy { it.value }.toSortedMap()

        depthMap.forEach { (depth, mids) ->
            println("depth: $depth has ${mids.size} trees")
        }

        val keyboard = Scanner(System.`in`)
        while (true) {
            val input = keyboard.nextLine()
            if (input == "quit") {
                break
            }
            val commands = input.split(" ")
            when (commands[0]) {
                "stages" -> println(
                    Blog.diffusionWidth(blogs[commands[1]]!!)
                        .toString()
                )
                "blogs_of_depth" -> println(
                    depthMap[commands[1].toInt()]!!
                        .map { it.key.mid }
                        .toString()
                )
                "longest_path" -> println(
                    Blog.longestPath(blogs[commands[1]]!!)
                        .toString()
                )
                else -> println("unknown command!")
            }
        }
    }

}