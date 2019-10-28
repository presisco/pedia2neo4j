package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.Neo4jGraph
import com.presisco.pedia2neo4j.getInt
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

    val midToId = hashMapOf<String, Int>()

    fun createReposts(root: String) {
        val childs = sqlite.select("select mid, repost from blog where repost_id = '$root'")
        val childMids = childs.map { it.getString("mid") }
        val ids = graph.createNodes(childMids, setOf("blog"))
        graph.createRelationFromIdToNodes(midToId[root]!!, ids, "repost")
        childMids.forEachIndexed { index, mid -> midToId[mid] = ids[index] }
        childs.filter { it.getInt("repost") > 0 }.forEach { createReposts(it.getString("mid")) }
    }

    @JvmStatic
    fun main(vararg args: String) {
        graph.clearGraph()
        graph.createNodes(roots, setOf("blog"))
            .forEachIndexed { index, id ->
                midToId[roots[index]] = id
            }
        roots.forEach { root ->
            createReposts(root)
        }
        graph.closeGraph()
    }
}