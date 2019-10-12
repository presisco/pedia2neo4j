package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.getString
import com.presisco.pedia2neo4j.toProperties
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

object BlogTreeAnalyze {

    val db = MapJdbcClient(
        HikariDataSource(
            HikariConfig(
                mapOf(
                    "dataSourceClassName" to "com.mysql.cj.jdbc.MysqlDataSource",
                    "dataSource.url" to "jdbc:mysql://10.144.48.95:3306/gossip?useUnicode=true&characterEncoding=UTF8",
                    "dataSource.user" to "root",
                    "dataSource.password" to "experimental",
                    "maximumPoolSize" to "1"
                ).toProperties()
            )
        )
    )

    fun main() {
        val iterator = db.selectIterator("select mid, repost_id from blog")
        val repostTrees = hashMapOf<String, Blog>()
        val roots = hashSetOf<Blog>()
        while (iterator.hasNext()) {
            val row = iterator.next()

            val mid = row.getString("mid")
            if (!repostTrees.containsKey(mid)) {
                repostTrees[mid] = Blog(mid)
                repostTrees[mid]!!.valid = true
            }

            if (row["repost_id"] != null) {
                roots.remove(repostTrees[mid]!!)
                val repostId = row.getString("repost_id")
                if (!repostTrees.containsKey(repostId)) {
                    repostTrees[repostId] = Blog(repostId)
                }
                roots.add(repostTrees[repostId]!!)
                repostTrees[repostId]!!.addChild(repostTrees[mid]!!)
            } else {
                roots.add(repostTrees[mid]!!)
            }
        }
    }

}