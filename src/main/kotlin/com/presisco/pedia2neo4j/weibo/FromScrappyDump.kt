package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.*
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

object FromScrappyDump {

    val scrappyClient = MapJdbcClient(
        HikariDataSource(
            HikariConfig(
                mapOf(
                    "dataSourceClassName" to "com.mysql.cj.jdbc.MysqlDataSource",
                    "dataSource.url" to "jdbc:mysql://localhost:3306/dump?useUnicode=true&characterEncoding=UTF8",
                    "dataSource.user" to "someone",
                    "dataSource.password" to "nopassword",
                    "maximumPoolSize" to "1"
                ).toProperties()
            )
        )
    )

    val sqliteClient = MapJdbcClient(
        HikariDataSource(
            HikariConfig(
                mapOf(
                    "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
                    "dataSource.url" to "jdbc:sqlite:D:/scrappy_weibo.db",
                    "maximumPoolSize" to "1"
                ).toProperties()
            )
        )
    )

    val numberRegex = Regex("\\d+")

    val repostTrees = hashMapOf<String, Blog>()

    val roots = hashSetOf<Blog>()

    @JvmStatic
    fun main(vararg args: String) {
        val earliestId = if (args.isNotEmpty()) {
            args[0].toLong()
        } else {
            500000L
        }
        val iterator = scrappyClient.selectIterator(
            "select * from scrapy_weibo_repost " +
                    "where id > ? and data like '{\"url\":%' " +
                    "order by id asc",
            earliestId
        )
        var maxId = 0L

        while (iterator.hasNext()) {
            val row = iterator.next()
            maxId = row.byType("id")

            val data = row.getString("data").json2Map() as HashMap<String, Any?>
            if (data["url"] == null) {
                continue
            }
            if (data["like"] != null) {
                data["like"] = data.getString("like").firstMatch(numberRegex)
            }
            if (data["repost"] != null) {
                data["repost"] = data.getString("repost").firstMatch(numberRegex)
            }
            if (data["comment"] != null) {
                data["comment"] = data.getString("comment").firstMatch(numberRegex)
            }
            val mid = MicroBlog.url2codedMid(data["url"] as String)
            data["mid"] = mid

            val meta = row.getString("meta").json2Map()
            val source = meta.getHashMap("user_data").getString("keyword")
            if (source.contains("//weibo.com")) {
                data["repost_link"] = source
                val repostId = MicroBlog.url2codedMid(source)
                data["repost_id"] = repostId
                if (!repostTrees.containsKey(mid)) {
                    repostTrees[mid] = Blog(mid)
                    repostTrees[mid]!!.valid = true
                    roots.remove(repostTrees[mid]!!)
                }
                if (!repostTrees.containsKey(repostId)) {
                    repostTrees[repostId] = Blog(repostId)
                    roots.add(repostTrees[repostId]!!)
                }
                repostTrees[repostId]!!.addChild(repostTrees[mid]!!)
            } else if (source.contains("//s.weibo.com")) {
                println("found repost tree root: ${data["mid"]}")
                data["repost_link"] = null
                data["repost_id"] = null
            }

            //sqliteClient.replace("blog", listOf(data))
        }

        println("max id: $maxId")
        println("unique blogs: ${repostTrees.keys.size}")
        println("trees: ${roots.size}")
        val diffusions = hashMapOf<String, List<Int>>()
        roots.forEach {
            diffusions[it.mid] = Blog.diffusionWidth(it)
        }
        println("max depth blog: ${diffusions.maxBy { it.value.size }}")

        val depthStats = diffusions.values.map { it.size }.groupBy { it }.mapValues { it.value.size }

        val mostDepth = depthStats.maxBy { it.value }
        println("most depth: $mostDepth")
    }

}