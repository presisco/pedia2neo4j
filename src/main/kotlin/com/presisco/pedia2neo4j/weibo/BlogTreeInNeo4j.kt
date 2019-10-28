package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.Neo4jGraph
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
    val roots = listOf("I9ttY0rZ1", "I9FFcxzpl")

    val blogInfos = hashMapOf<String, Blog>()

    val commentInfos = hashMapOf<String, Map<String, *>>()

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

    @JvmStatic
    fun main(vararg args: String) {
        graph.clearGraph()
        val iterator = sqlite.selectIterator(1, "select mid, repost_id, uid, time from blog")
        val uidSet = hashSetOf<String>()
        while (iterator.hasNext()) {
            val row = iterator.next()

            val mid = row.getString("mid")
            if (!blogInfos.containsKey(mid)) {
                blogInfos[mid] = Blog(mid)
            }
            blogInfos[mid]!!.uid = row.getString("uid")
            val timeStr = row["time"]
            blogInfos[mid]!!.time = if (timeStr != null && timeRegex.containsMatchIn(timeStr as String)) {
                timeStr.substring(0..10)
            } else {
                ""
            }

            if (row["repost_id"] != null) {
                val repostId = row.getString("repost_id")
                if (!blogInfos.containsKey(repostId)) {
                    blogInfos[repostId] = Blog(repostId)
                }
                blogInfos[repostId]!!.addChild(blogInfos[mid]!!)
            }
        }

        roots.forEach {
            midToId.putAll(createIdMap(childsSet(blogInfos[it]!!), "blog"))
        }

        roots.forEach {
            registerRepost(blogInfos[it]!!)
        }

        val commentIterator = sqlite.selectIterator(1, "select cid, mid, uid, time from comment")
        while (commentIterator.hasNext()) {
            val row = commentIterator.next()

            val cid = row.getString("cid")
            val mid = row.getString("mid")
            if (midToId.containsKey(mid)) {
                commentInfos[cid] = row
            }
        }

        cidToId.putAll(createIdMap(commentInfos, "comment"))

        commentInfos.forEach { (cid, info) ->
            val mid = midToId[info.getString("mid")]!!
            if (!midToCid.containsKey(mid)) {
                midToCid[mid] = arrayListOf()
            }
            midToCid[mid]!!.add(cidToId[cid]!!)
        }

        midToCid.forEach { (mid, cids) -> graph.createRelationFromIdToNodes(mid, cids, "reply") }

        midToId.forEach { (mid, _) ->
            uidSet.add(blogInfos[mid]!!.uid)
        }

        cidToId.forEach { (cid, _) ->
            uidSet.add(commentInfos[cid]!!.getString("uid"))
        }

        uidToId.putAll(createIdMap(uidSet, "user"))
        midToId.forEach { (mid, id) ->
            val uid = uidToId[blogInfos[mid]!!.uid]!!
            if (!uidToIds.containsKey(uid)) {
                uidToIds[uid] = arrayListOf()
            }
            uidToIds[uid]!!.add(id)
        }
        cidToId.forEach { (cid, id) ->
            val uid = uidToId[commentInfos[cid]!!.getString("uid")]!!
            if (!uidToIds.containsKey(uid)) {
                uidToIds[uid] = arrayListOf()
            }
            uidToIds[uid]!!.add(id)
        }
        uidToIds.forEach { (uid, ids) -> graph.createRelationFromIdToNodes(uid, ids, "create") }

        graph.closeGraph()
    }
}