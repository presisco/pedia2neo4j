package com.presisco.pedia2neo4j.weibo

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.getString
import com.presisco.pedia2neo4j.toProperties
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.io.File

object DumpGraphToText {

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

    val entityFile = "entity2id.txt"

    val relationFile = "relation2id.txt"

    val graphFile = "train2id.txt"

    val relations = listOf("keyword", "repost", "comment", "reference", "create")

    fun getDistinctEntities(table: String, column: String): List<String> {
        val iterator = db.selectIterator("select distinct $column from $table")
        val entities = arrayListOf<String>()
        while (iterator.hasNext()) {
            val entity = iterator.next().getString(column)
            entities.add("${table}_$entity")
        }
        return entities
    }

    fun getEntities(table: String, column: String): List<String> {
        val iterator = db.selectIterator("select $column from $table")
        val entities = arrayListOf<String>()
        while (iterator.hasNext()) {
            val entity = iterator.next()[column].toString()
            entities.add("${table}_$entity")
        }
        return entities
    }

    fun buildEntityIndex(vararg entityGroups: List<String>): Map<String, Int> {
        var index = 0
        val entityToIndex = hashMapOf<String, Int>()

        val writter = File(entityFile).bufferedWriter()

        entityGroups.forEach { group ->
            group.forEach { entity ->
                entityToIndex[entity] = index
                writter.write("$entity\t$index\n")
                index++
            }
        }
        writter.close()
        return entityToIndex
    }

    init {
        val writter = File(relationFile).bufferedWriter()
        relations.forEachIndexed { index, relation -> writter.write("$relation\t$index\n") }
        writter.close()
    }

    fun buildKeywordRelation(entityToIndex: Map<String, Int>): List<Triple<Int, Int, Int>> {
        val relationIndex = relations.indexOf("keyword")
        return db.buildSelect("mid", "keyword")
            .from("root")
            .execute()
            .map { row ->
                val from = row.getString("keyword")
                val to = row.getString("mid")
                Triple(entityToIndex["root_$from"]!!, entityToIndex["blog_$to"]!!, relationIndex)
            }
    }

    fun buildRepostRelation(entityToIndex: Map<String, Int>): List<Triple<Int, Int, Int>> {
        val relationIndex = relations.indexOf("repost")
        return db.buildSelect("mid", "repost_id")
            .from("blog")
            .where("repost_id", "is not", "null")
            .execute()
            .filter { row ->
                val from = row.getString("repost_id")
                if (!entityToIndex.containsKey("blog_$from")) {
                    println("unknown repost_id: $from")
                    false
                } else {
                    true
                }
            }.map { row ->
                val from = row.getString("repost_id")
                val to = row.getString("mid")
                Triple(entityToIndex["blog_$from"]!!, entityToIndex["blog_$to"]!!, relationIndex)
            }
    }

    fun buildCommentRelation(entityToIndex: Map<String, Int>): List<Triple<Int, Int, Int>> {
        val relationIndex = relations.indexOf("comment")
        return db.buildSelect("mid", "cid")
            .from("comment")
            .where("mid", "is not", "null")
            .execute()
            .map { row ->
                val from = row.getString("cid")
                val to = row.getString("mid")
                Triple(entityToIndex["comment_$from"]!!, entityToIndex["blog_$to"]!!, relationIndex)
            }
    }

    fun buildReferenceRelation(entityToIndex: Map<String, Int>): List<Triple<Int, Int, Int>> {
        val relationIndex = relations.indexOf("reference")
        return db.buildSelect("mid", "tid")
            .from("blog_with_tag")
            .execute()
            .map { row ->
                val from = row.getString("mid")
                val to = row["tid"].toString()
                Triple(entityToIndex["blog_$from"]!!, entityToIndex["tag_$to"]!!, relationIndex)
            }
    }

    fun buildCreateRelation(entityToIndex: Map<String, Int>): List<Triple<Int, Int, Int>> {
        val relationIndex = relations.indexOf("create")
        val relationList = arrayListOf<Triple<Int, Int, Int>>()
        relationList.addAll(db.buildSelect("mid", "uid")
            .from("blog")
            .where("uid", "is not", "null")
            .execute()
            .map { row ->
                val from = row.getString("uid")
                val to = row.getString("mid")
                Triple(entityToIndex["user_$from"]!!, entityToIndex["blog_$to"]!!, relationIndex)
            })
        relationList.addAll(db.buildSelect("cid", "uid")
            .from("comment")
            .where("uid", "is not", "null")
            .execute()
            .map { row ->
                val from = row.getString("uid")
                val to = row.getString("cid")
                Triple(entityToIndex["user_$from"]!!, entityToIndex["comment_$to"]!!, relationIndex)
            })
        return relationList
    }

    @JvmStatic
    fun main(vararg args: String) {
        val keywordEntities = getDistinctEntities("root", "keyword")
        val blogEntities = getEntities("blog", "mid")
        val userEntities = getEntities("user", "uid")
        val tagEntities = getEntities("tag", "tid")
        val commentEntities = getEntities("comment", "cid")

        val entityToIndex = buildEntityIndex(keywordEntities, blogEntities, userEntities, commentEntities, tagEntities)

        val relationSet = arrayListOf<Triple<Int, Int, Int>>()
        with(relationSet) {
            addAll(buildKeywordRelation(entityToIndex))
            addAll(buildRepostRelation(entityToIndex))
            addAll(buildCommentRelation(entityToIndex))
            addAll(buildReferenceRelation(entityToIndex))
            addAll(buildCreateRelation(entityToIndex))
        }

        val writter = File(graphFile).bufferedWriter()
        relationSet.forEach { triple -> writter.write("${triple.first}\t${triple.second}\t${triple.third}\n") }
        writter.close()
    }


}