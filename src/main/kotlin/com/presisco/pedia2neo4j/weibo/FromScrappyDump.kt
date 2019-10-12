package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.*
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*

object FromScrappyDump {

    val scrappyClient = MapJdbcClient(
        HikariDataSource(
            HikariConfig(
                mapOf(
                    "dataSourceClassName" to "com.mysql.cj.jdbc.MysqlDataSource",
                    "dataSource.url" to "jdbc:mysql://10.144.15.187:3815/spider?useUnicode=true&characterEncoding=UTF8",
                    "dataSource.user" to "spider",
                    "dataSource.password" to "QAZwsxEDC",
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
                    "dataSource.url" to "jdbc:sqlite:scrappy_weibo.db",
                    "maximumPoolSize" to "1"
                ).toProperties()
            )
        )
    )

    val numberRegex = Regex("\\d+")

    val repostTrees = hashMapOf<String, Blog>()

    val roots = hashSetOf<Blog>()

    init {
        sqliteClient.executeSQL(
            "create table if not exists blog(" +
                    "mid text(9) not null primary key, " +
                    "time text(16), " +
                    "content text, " +
                    "uid text not null, " +
                    "like integer, " +
                    "comment integer, " +
                    "repost integer, " +
                    "url text not null, " +
                    "repost_id text(9), " +
                    "repost_link text)"
        )
        sqliteClient.executeSQL(
            "create table if not exists transfer_id(" +
                    "max_id integer primary key)"
        )
    }

    @JvmStatic
    fun main(vararg args: String) {
        val idRecord = sqliteClient.select("select max(max_id) earliest from transfer_id")
        val earliestId = if (idRecord.isNotEmpty()) {
            idRecord.first().getInt("earliest")
        } else {
            0
        }
        val iterator = scrappyClient.selectIterator(
            "select * from scrapy_weibo_repost " +
                    "where id > ? and data like '{\"url\":%' " +
                    "order by id desc",
            earliestId
        )
        var maxId = 0L

        while (iterator.hasNext()) {
            val row = iterator.next()
            maxId = Math.max(maxId, row.byType("id"))

            val data = row.getString("data").json2Map() as HashMap<String, Any?>
            val rectified = hashMapOf<String, Any?>()
            if (data["url"] == null) {
                continue
            }
            rectified["url"] = data["url"]
            if (data["like"] != null) {
                rectified["like"] = data.getString("like").firstMatch(numberRegex)
            }
            if (data["repost"] != null) {
                rectified["repost"] = data.getString("repost").firstMatch(numberRegex)
            }
            if (data["comment"] != null) {
                rectified["comment"] = data.getString("comment").firstMatch(numberRegex)
            }
            val mid = MicroBlog.url2codedMid(data["url"] as String)
            rectified["mid"] = mid
            val uid = MicroBlog.uidFromBlogUrl(data["url"] as String)
            rectified["uid"] = uid
            rectified["time"] = data["create_time"]

            row["meta"] ?: continue
            val meta = row.getString("meta").json2Map()
            val source = meta.getHashMap("user_data").getString("keyword")
            if (source.contains("//weibo.com")) {
                rectified["repost_link"] = source
                val repostId = MicroBlog.url2codedMid(source)
                rectified["repost_id"] = repostId
                if (!repostTrees.containsKey(mid)) {
                    repostTrees[mid] = Blog(mid)
                    repostTrees[mid]!!.valid = true
                    roots.remove(repostTrees[mid]!!)
                    sqliteClient.replace("blog", listOf(rectified))
                }
                if (!repostTrees.containsKey(repostId)) {
                    repostTrees[repostId] = Blog(repostId)
                    roots.add(repostTrees[repostId]!!)
                }
                repostTrees[repostId]!!.addChild(repostTrees[mid]!!)
            } else if (source.contains("//s.weibo.com")) {
                println("found repost tree root: ${data["mid"]}")
                rectified["repost_link"] = null
                rectified["repost_id"] = null
                sqliteClient.replace("blog", listOf(rectified))
            }
        }

        println("max id: $maxId")
        sqliteClient.insert("transfer_id", listOf(mapOf("max_id" to maxId)))
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