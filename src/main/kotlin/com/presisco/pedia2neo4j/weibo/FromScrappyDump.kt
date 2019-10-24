package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.*
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.time.LocalDateTime
import java.util.*

object FromScrappyDump {

    val scrappyClient = MapJdbcClient(
        HikariDataSource(
            HikariConfig(
                mapOf(
                    "dataSourceClassName" to "com.mysql.cj.jdbc.MysqlDataSource",
                    "dataSource.url" to "jdbc:mysql://localhost:3306/scrappy?useUnicode=true&characterEncoding=UTF8&useCursorFetch=true",
                    "dataSource.user" to "root",
                    "dataSource.password" to "experimental",
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

    val numberRegex = Regex("\\D*?([0-9]+)")
    val quoteUserRegex = Regex("(@\\S+)[:\\s]*")
    val topicRegex = Regex("(#.+?#)")

    val repostTrees = hashMapOf<String, Blog>()

    val roots = hashSetOf<Blog>()

    init {
        sqliteClient.executeSQL(
            "create table if not exists blog(" +
                    "mid text(9) not null primary key, " +
                    "scrap_time text(20), " +
                    "time text(16), " +
                    "content text, " +
                    "uid text(32) not null, " +
                    "like integer, " +
                    "comment integer, " +
                    "repost integer, " +
                    "url text not null, " +
                    "repost_id text(9), " +
                    "repost_link text)"
        )
        sqliteClient.executeSQL(
            "create table if not exists user(" +
                    "uid text(32) not null primary key, " +
                    "name text(64))"
        )
        sqliteClient.executeSQL(
            "create table if not exists tag(" +
                    "mid text(9), " +
                    "tag text)"
        )
        sqliteClient.executeSQL(
            "create table if not exists root(" +
                    "mid text(9) not null primary key, " +
                    "keyword text not null)"
        )
        sqliteClient.executeSQL(
            "create table if not exists comment(" +
                    "cid text(9) not null primary key, " +
                    "mid text(9) not null, " +
                    "like integer, " +
                    "content text not null, " +
                    "uid text(32) not null, " +
                    "scrap_time text(20), " +
                    "time text(16))"
        )
        sqliteClient.executeSQL(
            "create table if not exists transfer_id(" +
                    "max_id integer primary key)"
        )
    }

    fun containsMid(mid: String): String? {
        val result = sqliteClient.select("select scrap_time from blog where mid = ?", mid)
        if (result.isEmpty()) {
            return null
        } else {
            return result.first()["scrap_time"] as String
        }
    }

    fun containsUid(uid: String): Boolean {
        val result = sqliteClient.select("select uid from user where uid = ?", uid)
        return result.isNotEmpty()
    }

    fun containsCid(cid: String): String? {
        val result = sqliteClient.select("select scrap_time from comment where cid = ?", cid)
        if (result.isEmpty()) {
            return null
        } else {
            return result.first()["scrap_time"] as String
        }
    }

    fun updateBlog(blog: Map<String, *>) {
        sqliteClient.update("blog")
            .set(
                "like" to blog["like"],
                "repost" to blog["repost"],
                "comment" to blog["comment"]
            )
            .where("mid", "=", blog["mid"])
            .execute()
    }

    fun updateComment(comment: Map<String, *>) {
        sqliteClient.update("comment")
            .set("like" to comment.getInt("like"))
            .where("cid", "=", comment.getString("cid"))
            .execute()
    }

    fun parseBlog(scrapTime: String, data: Map<String, *>, meta: Map<String, *>): Boolean {
        val rectified = hashMapOf<String, Any?>()
        if (data["url"] == null) {
            println("no url")
            return false
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
        if (data["content"] != null) {
            rectified["content"] = data.getString("content")
        }
        if (data["text"] != null) {
            rectified["content"] = data.getString("text")
        }
        val mid = MicroBlog.url2codedMid(data["url"] as String)
        if (mid == "") {
            println("null mid")
            return false
        }
        rectified["mid"] = mid
        val uid = MicroBlog.uidFromBlogUrl(data["url"] as String)
        rectified["uid"] = uid
        val username = data["user"]
        if (data.containsKey("create_time")) {
            rectified["time"] = data["create_time"]
        }
        if (data["create_time"] == null) {
            println("unknown time!")
        }
        rectified["scrap_time"] = scrapTime

        if (containsMid(mid) == null && rectified["content"] != null) {
            val userQuotes = rectified.getString("content")
                .substringBefore("//@")
                .extractValues(quoteUserRegex)
            val topics = rectified.getString("content")
                .substringBefore("//@")
                .extractValues(topicRegex)
            sqliteClient.insert("tag", userQuotes.map {
                mapOf(
                    "mid" to mid,
                    "tag" to it
                )
            })
            sqliteClient.insert("tag", topics.map {
                mapOf(
                    "mid" to mid,
                    "tag" to it
                )
            })
        }

        val source = meta.getHashMap("user_data").getString("keyword")
        if (source.contains("//weibo.com")) {
            rectified["repost_link"] = source
            val repostId = MicroBlog.url2codedMid(source)
            rectified["repost_id"] = repostId
            if (!repostTrees.containsKey(mid)) {
                repostTrees[mid] = Blog(mid)
                repostTrees[mid]!!.valid = true
            }
            val lastScrapTime = containsMid(mid)
            if (lastScrapTime == null) {
                sqliteClient.insert("blog", listOf(rectified))
            } else if (lastScrapTime < scrapTime) {
                updateBlog(rectified)
            }
            if (!containsUid(uid)) {
                sqliteClient.insert(
                    "user", listOf(
                        mapOf(
                            "uid" to uid,
                            "name" to username
                        )
                    )
                )
            }

            if (!repostTrees.containsKey(repostId)) {
                repostTrees[repostId] = Blog(repostId)
                roots.add(repostTrees[repostId]!!)
            } else {
                roots.remove(repostTrees[mid]!!)
            }
            repostTrees[repostId]!!.addChild(repostTrees[mid]!!)
        } else {
            rectified["repost_link"] = null
            rectified["repost_id"] = null
            if (!repostTrees.containsKey(mid)) {
                repostTrees[mid] = Blog(mid)
                repostTrees[mid]!!.valid = true
            }
            if (!repostTrees[mid]!!.valid) {
                repostTrees[mid]!!.valid = true
            }
            val lastScrapTime = containsMid(mid)
            if (lastScrapTime == null) {
                sqliteClient.insert("blog", listOf(rectified))
                sqliteClient.insert(
                    "root", listOf(
                        mapOf(
                            "mid" to mid,
                            "keyword" to source
                        )
                    )
                )
            } else if (scrapTime > lastScrapTime) {
                updateBlog(rectified)
            }
            roots.add(repostTrees[mid]!!)
        }
        return true
    }

    fun <T> Map<String, *>.maybeFirstOfList(key: String): T {
        return if (this[key] is List<*>) {
            this.getList<T>("comment_id")[0]
        } else {
            this[key] as T
        }
    }

    fun parseComment(scrapTime: String, data: Map<String, *>, meta: Map<String, *>): Boolean {
        if (data.containsKey("all")) {
            println("bad comment!")
            return false
        }
        val rectified = hashMapOf<String, Any?>()
        rectified["cid"] = MicroBlog.encodeMid(data.maybeFirstOfList("comment_id"))

        if (data["create_time"] == null) {
            rectified["time"] = null
        } else {
            val timeString = data.maybeFirstOfList<String>("create_time")
            rectified["time"] = if (timeString.startsWith("<div")) {
                timeString.substringAfter(">")
                    .substringBefore("<")
            } else {
                timeString
            }
        }

        rectified["scrap_time"] = scrapTime
        rectified["content"] = data["content"]

        rectified["uid"] = MicroBlog.uidFromUserUrl(data.maybeFirstOfList("user_link"))
        rectified["mid"] = MicroBlog.url2codedMid(meta.getHashMap("user_data").getString("keyword"))
        rectified["like"] = data.maybeFirstOfList<String>("like").firstMatch(numberRegex)
        val lastScrapTime = containsCid(rectified.getString("cid"))
        if (lastScrapTime == null) {
            sqliteClient.insert("comment", listOf(rectified))
        } else if (scrapTime > lastScrapTime) {
            updateComment(rectified)
        }
        return true
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
            Int.MIN_VALUE,
            "select * from scrapy_weibo_repost " +
                    "order by id asc"
        )
        var maxId = 0L

        while (iterator.hasNext()) {
            val row = iterator.next()
            maxId = row.byType("id")
            if (row["data"] == null || row["meta"] == null) {
                println("row at id: $maxId is completely broken!")
                continue
            }

            val scrapTime = row.byType<LocalDateTime>("createtime").toFormatString()
            val data = row.getString("data").json2Map() as HashMap<String, Any?>
            val meta = row.getString("meta").json2Map()

            val result =
                try {
                    when (row.getString("version")) {
                        "repost" -> parseBlog(scrapTime, data, meta)
                        "comment1" -> parseComment(scrapTime, data, meta)
                        else -> {
                            println("unexpected version: ${row.getString("version")}")
                            false
                        }
                    }
                } catch (e: Exception) {
                    println(e.message)
                    false
                }
            if (!result) {
                println("parse failed for id: ${row["id"]}")
            }
        }

        println("max id: $maxId")
        if (maxId != 0L) {
            sqliteClient.insert("transfer_id", listOf(mapOf("max_id" to maxId)))
        }
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