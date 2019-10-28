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
                    "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
                    "dataSource.url" to "jdbc:sqlite:D:/scrappy_dump.db",
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

    val numberRegex = Regex(".*?([0-9]+)")
    val numberFromXml = Regex(">([0-9]+)<")
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

    fun Map<String, *>.intOrZero(key: String): Int {
        return if (this[key] != null) {
            val number = this.getString(key).firstMatch(numberRegex)
            if (number == null) {
                0
            } else {
                number.toInt()
            }
        } else {
            0
        }
    }

    fun parseBlog(scrapTime: String, data: Map<String, *>, meta: Map<String, *>): Boolean {
        val rectified = hashMapOf<String, Any?>()
        if (data["url"] == null) {
            println("no url")
            return false
        }
        rectified["url"] = data["url"]
        rectified["repost"] = data.intOrZero("repost")
        rectified["comment"] = data.intOrZero("comment")
        rectified["like"] = data.intOrZero("like")
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
        } else {
            rectified["time"] = data.getString("time")
                .substringAfter("\">")
                .substringBefore("</a>")
                .trim()
        }
        if (data["create_time"] == null) {
            println("unknown time!")
        }
        rectified["scrap_time"] = scrapTime

        val lastScrapTime = containsMid(mid)
        if (lastScrapTime == null && rectified["content"] != null) {
            val userQuotes = rectified.getString("content")
                .substringBefore("//@")
                .extractValues(quoteUserRegex)
            val topics = rectified.getString("content")
                .substringBefore("//@")
                .extractValues(topicRegex)
            WeiboManager.setList("tag", mid, userQuotes)
            WeiboManager.setList("tag", mid, topics)
        }

        if (!containsUid(uid)) {
            WeiboManager.setInfo(
                "user", uid, mapOf(
                    "uid" to uid,
                    "name" to username
                )
            )
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
            }
            repostTrees[mid]!!.valid = true
            if (lastScrapTime == null) {
                WeiboManager.setInfo(
                    "root", mid, mapOf(
                        "mid" to mid,
                        "keyword" to source
                    )
                )
            }
            roots.add(repostTrees[mid]!!)
        }
        WeiboManager.setInfo("blog", mid, rectified)
        return true
    }

    fun <T> Map<String, *>.maybeFirstOfList(key: String): T {
        return if (this[key] is List<*>) {
            this.getList<T>(key)[0]
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
        val cid = MicroBlog.encodeMid(data.maybeFirstOfList("comment_id"))
        rectified["cid"] = cid

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

        val uid = MicroBlog.uidFromUserUrl(data.maybeFirstOfList("user_link"))
        rectified["uid"] = uid
        rectified["mid"] = MicroBlog.url2codedMid(meta.getHashMap("user_data").getString("keyword"))
        val likeText = if (data["like"] != null) {
            data.maybeFirstOfList<String>("like").firstMatch(numberRegex)
        } else {
            null
        }
        rectified["like"] = if (likeText != null && likeText != "") {
            likeText.toInt()
        } else {
            0
        }
        WeiboManager.setInfo("comment", cid, rectified)

        return true
    }

    @JvmStatic
    fun main(vararg args: String) {
        val idRecord = sqliteClient.select("select max(max_id) earliest from transfer_id")
        var earliestId = if (idRecord.isNotEmpty()) {
            idRecord.first().getInt("earliest")
        } else {
            -1
        }
        println("starting at id: ${earliestId + 1}")
        val iterator = scrappyClient.selectIterator(
            1000,
            "select * from data " +
                    "where id > $earliestId"
        )
        var counter = 0
        var maxId = 0L
        while (iterator.hasNext()) {
            counter++
            val row = iterator.next()
            maxId = row.byType("id")
            if (row["data"] == null || row["meta"] == null) {
                println("row at id: $maxId is completely broken!")
                continue
            }

            val scrapTime = row.getString("createtime")
            val data = row.getString("data").json2Map() as HashMap<String, Any?>
            val meta = row.getString("meta").json2Map()


            try {
                val result = when (row.getString("version")) {
                    "repost", "search1" -> parseBlog(scrapTime, data, meta)
                    "comment1" -> parseComment(scrapTime, data, meta)
                    else -> {
                        println("unexpected version: ${row.getString("version")}")
                        false
                    }
                }
                if (!result) {
                    println("parse failed for id: ${row["id"]}")
                }
            } catch (e: Exception) {
                println("exception at id: ${row["id"]}")
                throw e
            }

            if (counter % 500000 == 0) {
                sqliteClient.replace("blog", WeiboManager.getInfoValues("blog").toList())

                sqliteClient.replace("comment", WeiboManager.getInfoValues("comment").toList())

                sqliteClient.replace("user", WeiboManager.getInfoValues("user").toList())

                sqliteClient.replace("tag", WeiboManager.getFlattenList("tag", "mid", "tag"))

                sqliteClient.replace("root", WeiboManager.getInfoValues("root").toList())

                WeiboManager.reset()
            }
        }

        println("max id: $maxId, read $counter records!")

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