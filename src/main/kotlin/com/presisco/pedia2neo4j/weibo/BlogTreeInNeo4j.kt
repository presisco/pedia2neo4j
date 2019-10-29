package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.Neo4jGraph
import com.presisco.pedia2neo4j.addToValue
import com.presisco.pedia2neo4j.getString
import com.presisco.pedia2neo4j.toProperties
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

object BlogTreeInNeo4j {

    val graph = Neo4jGraph()
    val sqlite = MapJdbcClient(
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
    val roots = listOf("I9ttY0rZ1")

    val blogInfos = hashMapOf<String, Blog>()

    val commentInfos = hashMapOf<String, Map<String, *>>()

    val tags = hashMapOf<String, MutableList<Int>>()

    val tagToId = hashMapOf<String, Int>()

    val times = hashMapOf<String, MutableList<Int>>()

    val timeToId = hashMapOf<String, Int>()

    val midToId = hashMapOf<String, Int>()

    val midToCid = hashMapOf<Int, MutableList<Int>>()

    val cidToId = hashMapOf<String, Int>()

    val uidToId = hashMapOf<String, Int>()

    val uidToIds = hashMapOf<Int, MutableList<Int>>()

    fun childsSet(blog: Blog): Set<String> {
        val set = hashSetOf(blog.mid)
        blog.childs.forEach {
            set.addAll(childsSet(it))
        }
        return set
    }

    fun registerRepost(blog: Blog) {
        if (blog.childs.isEmpty()) {
            return
        }
        val ids = blog.childs.map { midToId[it.mid]!! }
        graph.createRelationFromIdToNodes(midToId[blog.mid]!!, ids, "repost")
        blog.childs.forEach { registerRepost(it) }
    }

    fun createIdMap(strId: Set<String>, label: String): Map<String, Int> {
        val idList = strId.toList()
        val idMap = hashMapOf<String, Int>()
        graph.createNodes(idList, setOf(label))
            .forEachIndexed { index, id ->
                idMap[idList[index]] = id
            }
        return idMap
    }

    fun createIdMap(strId: Map<String, *>, label: String) = createIdMap(strId.keys, label)

    val timeRegex = "\\d{4}-\\d{2}-\\d{2}".toRegex()

    fun buildTrees() {
        val iterator = sqlite.selectIterator(1, "select mid, repost_id, uid, time from blog")
        while (iterator.hasNext()) {
            val row = iterator.next()

            val mid = row.getString("mid")
            if (!blogInfos.containsKey(mid)) {
                blogInfos[mid] = Blog(mid)
            }
            blogInfos[mid]!!.uid = row.getString("uid")
            val timeStr = row["time"]
            blogInfos[mid]!!.time = if (timeStr != null && timeRegex.containsMatchIn(timeStr as String)) {
                timeStr.substring(0..9)
            } else {
                "unknown"
            }

            if (row["repost_id"] != null) {
                val repostId = row.getString("repost_id")
                if (!blogInfos.containsKey(repostId)) {
                    blogInfos[repostId] = Blog(repostId)
                }
                blogInfos[repostId]!!.addChild(blogInfos[mid]!!)
            }
        }
    }

    fun loadComments() {
        val iterator = sqlite.selectIterator(1, "select cid, mid, uid, time from comment")
        while (iterator.hasNext()) {
            val row = iterator.next()

            val cid = row.getString("cid")
            val mid = row.getString("mid")
            if (midToId.containsKey(mid)) {
                commentInfos[cid] = row
            }
        }
    }

    fun loadTags() {
        val iterator = sqlite.selectIterator(1, "select mid, tag from tag")
        while (iterator.hasNext()) {
            val row = iterator.next()

            val tag = row.getString("tag")
            val mid = row.getString("mid")
            if (midToId.containsKey(mid)) {
                tags.addToValue(tag, midToId[mid]!!)
            }
        }
    }

    @JvmStatic
    fun main(vararg args: String) {
        graph.clearGraph()

        buildTrees()

        val relatedMids = hashSetOf<String>()
        roots.forEach {
            val childMids = childsSet(blogInfos[it]!!)
            midToId.putAll(createIdMap(childMids, "blog"))
            relatedMids.addAll(childMids)
        }

        roots.forEach {
            registerRepost(blogInfos[it]!!)
        }

        loadComments()

        cidToId.putAll(createIdMap(commentInfos, "comment"))

        commentInfos.forEach { (cid, info) ->
            val mid = midToId[info.getString("mid")]!!
            midToCid.addToValue(mid, cidToId[cid]!!)
        }

        midToCid.forEach { (mid, cids) -> graph.createRelationFromIdToNodes(mid, cids, "reply") }

        val uidSet = hashSetOf<String>()
        midToId.forEach { (mid, _) ->
            uidSet.add(blogInfos[mid]!!.uid)
        }

        cidToId.forEach { (cid, _) ->
            uidSet.add(commentInfos[cid]!!.getString("uid"))
        }

        uidToId.putAll(createIdMap(uidSet, "user"))
        midToId.forEach { (mid, id) ->
            val uid = uidToId[blogInfos[mid]!!.uid]!!
            uidToIds.addToValue(uid, id)
        }
        cidToId.forEach { (cid, id) ->
            val uid = uidToId[commentInfos[cid]!!.getString("uid")]!!
            if (!uidToIds.containsKey(uid)) {
                uidToIds[uid] = arrayListOf()
            }
            uidToIds[uid]!!.add(id)
        }
        uidToIds.forEach { (uid, ids) -> graph.createRelationFromIdToNodes(uid, ids, "create") }

        loadTags()
        tagToId.putAll(createIdMap(tags, "tag"))
        tags.forEach { (tag, mids) -> graph.createRelationFromIdToNodes(tagToId[tag]!!, mids, "contain") }

        relatedMids.forEach {
            val time = blogInfos[it]!!.time
            times.addToValue(time, midToId[blogInfos[it]!!.mid]!!)
        }
        /*
        commentInfos.forEach { cid, info ->
            val time = info.getString("time")
            times.addToValue(time, cidToId[cid]!!)
        }
        */
        timeToId.putAll(createIdMap(times, "time"))
        times.forEach { (time, ids) -> graph.createRelationFromIdToNodes(timeToId[time]!!, ids, "when") }

        graph.closeGraph()
    }
}