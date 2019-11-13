package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.*
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FromScrappyDump {

    val scrappyClient = MapJdbcClient(
        HikariDataSource(
            HikariConfig(
                mapOf(
                    "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
                    "dataSource.url" to "jdbc:sqlite:E:/database/scrappy_dump.db",
                    "maximumPoolSize" to "1"
                ).toProperties()
            )
        )
    )

    val weiboClient = MapJdbcClient(
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

    val numberRegex = Regex(".*?([0-9]+)")
    val timeFromXml = Regex("title=\"(.+?)\"")
    val timeFromXmlText = Regex(">(.+?)</")
    val quoteUserRegex = Regex("(@\\S+)[:\\s]*")
    val topicRegex = Regex("(#.+?#)")
    val nicknameRegex = Regex("nick-name=\"(.+?)\" ")

    val scrapTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")


    val repostTrees = hashMapOf<String, Blog>()

    val roots = hashSetOf<Blog>()

    val tags = hashMapOf<String, Int>()

    val blogUids = hashSetOf<String>()

    val blogMids = hashSetOf<String>()

    init {
        val tagIter = weiboClient.selectIterator("select * from tag")
        while (tagIter.hasNext()) {
            val row = tagIter.next()
            tags[row.getString("tag")] = row.getInt("tid")
        }
        val blogIter = weiboClient.selectIterator("select mid, uid from blog")
        while (blogIter.hasNext()) {
            val row = blogIter.next()
            blogMids.add(row.getString("mid"))
            blogUids.add(row.getString("uid"))
        }
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

    fun detectTags(content: String): List<Int> {
        val topContent = content.substringBefore("//@")
        val tagContent = arrayListOf<String>()
        tagContent.addAll(topContent.extractValues(quoteUserRegex))
        tagContent.addAll(topContent.extractValues(topicRegex))
        return tagContent.map { tag ->
            if (!tags.containsKey(tag))
                tags[tag] = tags.size
            tags[tag]!!
        }
    }

    fun saveTags() {
        val insertList = arrayListOf<Map<String, Any>>()
        tags.forEach { (tag, tid) ->
            insertList.add(
                mapOf(
                    "tid" to tid,
                    "tag" to tag
                )
            )
        }
        weiboClient.replace("tag", insertList)
    }

    val minuteRegex = Regex("(\\d+)分钟.+?")
    val hourRegex = Regex("(\\d+)小时.+?")
    val hourAndMinuteDay = Regex("今天\\s?(\\d{2}):(\\d{2}).*")
    val monthAndDayRegex = Regex("(\\d+)月(\\d+)日 (\\d{2}):(\\d{2}).*")
    val validOutputFormat = Regex("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}")

    fun String.toSystemMs() = LocalDateTime.parse(this, scrapTimeFormatter)

    fun alignTime(scrapTime: String, time: String): String {
        val scrapDateTime = scrapTime.toSystemMs()

        return if (time.contains("刚刚")) {
            timeFormatter.format(scrapDateTime)
        } else if (time.contains("秒")) {
            timeFormatter.format(scrapDateTime)
        } else if (time.contains("分钟前")) {
            timeFormatter.format(
                scrapDateTime.minusMinutes(
                    time.firstMatch(minuteRegex)!!.toLong()
                )
            )
        } else if (time.contains("小时前")) {
            timeFormatter.format(
                scrapDateTime.minusHours(
                    time.firstMatch(hourRegex)!!.toLong()
                )
            )
        } else if (time.contains("今天")) {
            val values = hourAndMinuteDay.matchEntire(time)!!.groupValues
            timeFormatter.format(
                scrapDateTime.withHour(values[1].toInt())
                    .withMinute(values[2].toInt())
            )
        } else if (time.contains("年")) {
            time.replace("年", "-")
                .replace("月", "-")
                .replace("日", "")
        } else if (time.contains(monthAndDayRegex)) {
            val values = monthAndDayRegex.matchEntire(time)!!.groupValues
            timeFormatter.format(
                scrapDateTime.withMonth(values[1].toInt())
                    .withDayOfMonth(values[2].toInt())
                    .withHour(values[3].toInt())
                    .withMinute(values[4].toInt())
            )
        } else {
            time
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
        val username = if (data["name"] != null) {
            if (data.getString("name").contains("<a ")) {
                data.getString("name").extractValues(nicknameRegex).first().trim()
            } else {
                data.getString("name").trim()
            }
        } else if (data["user"] != null) {
            data.getString("user").trim()
        } else {
            "unknown"
        }

        if (username != null && username.length > 30) {
            println("username: $username too long!")
        }

        if (data.containsKey("create_time")) {
            val timeString = data["create_time"] as String?
            if (timeString == null) {
                rectified["time"] = "unknown"
            } else if (timeString.contains("<div")) {
                rectified["time"] = data.getString("create_time").extractValues(timeFromXml)
                    .first()
                    .trim()
            } else {
                rectified["time"] = timeString
            }
        } else if (data.containsKey("time")) {
            val timeString = data.getString("time").replace("\n", "")
            if (timeString.contains("<a")) {
                rectified["time"] = timeString.extractValues(timeFromXmlText)
                    .first()
                    .substringBefore(" 转赞人数")
                    .trim()
            } else {
                rectified["time"] = timeString.trim()
            }
        } else {
            return false
        }

        rectified["time"] = alignTime(scrapTime, rectified.getString("time"))
        if (!rectified.getString("time").matches(validOutputFormat)
            && rectified.getString("time") != "unknown"
        ) {
            println("bad time: ${rectified.getString("time")}")
        }

        rectified["scrap_time"] = scrapTime

        if (!blogMids.contains(mid) && rectified["content"] != null) {
            val tids = detectTags(rectified.getString("content"))
            WeiboManager.setList("blog_with_tag", mid, tids)
        }

        if (!blogUids.contains(uid)) {
            WeiboManager.setInfo(
                "user", uid, mapOf(
                    "uid" to uid,
                    "name" to username
                )
            )
            blogUids.add(uid)
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
            WeiboManager.setInfo(
                "root", mid, mapOf(
                    "mid" to mid,
                    "keyword" to source
                )
            )
            roots.add(repostTrees[mid]!!)
        }
        WeiboManager.setInfo("blog", mid, rectified)
        blogMids.add(mid)
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
            var timeString = data.maybeFirstOfList<String>("create_time")
            timeString = if (timeString.startsWith("<div")) {
                timeString.substringAfter(">")
                    .substringBefore("<")
            } else {
                timeString
            }
            if (timeString.contains("楼")) {
                timeString = timeString.substringAfter("楼 ")
            }
            rectified["time"] = timeString
        }

        rectified["scrap_time"] = scrapTime
        rectified["content"] = data["content"]

        val uid = MicroBlog.uidFromUserUrl(data.maybeFirstOfList("user_link"))
        rectified["uid"] = uid
        if (!blogUids.contains(uid)) {
            WeiboManager.setInfo(
                "user", uid, mapOf(
                    "uid" to uid,
                    "name" to "unknown_comment_user"
                )
            )
        }

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
        var earliestId = -1
        println("starting at id: ${earliestId + 1}")
        val iterator = scrappyClient.selectIterator(
            200000,
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
                weiboClient.replace("blog", WeiboManager.getInfoValues("blog").toList())

                weiboClient.replace("comment", WeiboManager.getInfoValues("comment").toList())

                weiboClient.replace("blog_user", WeiboManager.getInfoValues("user").toList())

                saveTags()

                weiboClient.replace("blog_with_tag", WeiboManager.getFlattenList("blog_with_tag", "mid", "tid"))

                weiboClient.replace("root", WeiboManager.getInfoValues("root").toList())

                WeiboManager.reset()
            }
        }

        println("max id: $maxId, read $counter records!")

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