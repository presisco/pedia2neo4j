package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.createRelationBetweenIds
import com.presisco.pedia2neo4j.mergeEntity
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.GraphDatabase

val neo4jConf = mapOf(
    "uri" to "bolt://localhost:7687",
    "username" to "neo4j",
    "password" to "experimental"
)
val neo4jDriver = GraphDatabase.driver(
    neo4jConf["uri"],
    AuthTokens.basic(neo4jConf["username"], neo4jConf["password"]),
    Config.defaultConfig()
)!!
val session = neo4jDriver.session()
val sqliteConf = mapOf(
    "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
    "dataSource.url" to "jdbc:sqlite:H:/weibo_dump.db",
    "maximumPoolSize" to "1"
)
val client = MapJdbcClient(HikariDataSource(HikariConfig(sqliteConf.toProperties())))

val weiboIdMap = hashMapOf<String, Int>()

fun mergeEntityWithCache(mid: String, type: String): Int {
    if (!weiboIdMap.containsKey(mid)) {
        weiboIdMap[mid] = session.mergeEntity(mid, setOf(type))
    }
    return weiboIdMap[mid]!!
}

fun setBlogPropsForId(id: Int, props: Map<String, *>) {
    val propKeys = setOf("like", "create_time", "comment", "repost")

    session.writeTransaction {
        it.run("match (p)" +
                " where id(p) = $id" +
                " set ${propKeys.joinToString(separator = ", ") { key -> "p.$key = \$$key" }}"
            , props.filterKeys { key -> propKeys.contains(key) })
    }
}

fun setCommentPropsForId(id: Int, props: Map<String, *>) {
    val propKeys = setOf("like", "create_time")

    session.writeTransaction {
        it.run("match (p)" +
                " where id(p) = $id" +
                " set ${propKeys.joinToString(separator = ", ") { key -> "p.$key = \$$key" }}"
            , props.filterKeys { key -> propKeys.contains(key) })
    }
}

fun main() {
    session.writeTransaction {
        it.run("match (n) detach delete n")
    }

    println("inserting blogs")
    client.buildSelect("id", "uid", "repost", "like", "comment", "repost_id", "create_time")
        .from("blog")
        .execute()
        .forEach { blog ->
            val blogNeoId = mergeEntityWithCache(blog["id"] as String, "blog")
            setBlogPropsForId(blogNeoId, blog)

            if (blog["repost_id"] != null) {
                val repostId = blog["repost_id"] as String
                val repostNeoId = mergeEntityWithCache(repostId, "blog")
                session.createRelationBetweenIds(repostNeoId, blogNeoId, "转发")
            }
        }

    println("inserting comments")
    client.buildSelect("id", "uid", "blog_id", "like", "create_time")
        .from("comment")
        .execute()
        .forEach { comment ->
            val commentNeoId = mergeEntityWithCache(comment["id"] as String, "comment")
            setCommentPropsForId(commentNeoId, comment)
            val blogNeoId = mergeEntityWithCache(comment["blog_id"] as String, "blog")
            session.createRelationBetweenIds(blogNeoId, commentNeoId, "评论")
        }

    println("finished!")
}