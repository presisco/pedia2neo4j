package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.toProperties
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

object TreePatternAnalyze {

    val stepSize = 10

    val db = MapJdbcClient(
        HikariDataSource(
            HikariConfig(
                mapOf(
                    "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
                    "dataSource.url" to "jdbc:sqlite:E:/database/scrappy_weibo.db",
                    "maximumPoolSize" to "1"
                ).toProperties()
            )
        )
    )

    @JvmStatic
    fun main(vararg args: String) {
        val iterator = db.selectIterator("select mid, repost_id from blog")

        val woods = Blog.buildWoods(iterator)
        val blogs = woods.first
        val roots = woods.second

        roots.forEach { Blog.maxDepth(it) }

        val filtered = roots.filter { it.maxDepth > 4 }

        val stages = filtered.map {
            val diffusion = Blog.diffusionWidth(it)
            println("mid ${it.mid}: $diffusion")
            diffusion.average().toInt()
        }
        val counters = hashMapOf<Int, Int>()

        stages.forEach {
            val tier = it / stepSize
            if (!counters.containsKey(tier)) {
                counters[tier] = 0
            }
            counters[tier] = counters[tier]!! + 1
        }

        counters.forEach { (tier, count) ->
            println("tier ${tier * stepSize} - ${(tier + 1) * stepSize}: $count")
        }

    }

}