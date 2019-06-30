package com.presisco.pedia2neo4j.cndbpedia

import com.github.kittinunf.fuel.httpGet
import com.presisco.gsonhelper.ListHelper
import com.presisco.lazyjdbc.client.MapJdbcClient
import com.presisco.pedia2neo4j.json2Map
import com.presisco.pedia2neo4j.nowMs
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object APIRequestCache {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    fun now() = formatter.format(LocalDateTime.now())

    val sqliteConf = mapOf(
        "dataSourceClassName" to "org.sqlite.SQLiteDataSource",
        "dataSource.url" to "jdbc:sqlite:cn-dbpedia_request_cache.db",
        "maximumPoolSize" to "1"
    )
    val client = MapJdbcClient(HikariDataSource(HikariConfig(sqliteConf.toProperties())))

    const val tripleUrl = "http://shuyantech.com/api/cndbpedia/avpair?q="
    const val mentionUrl = "http://shuyantech.com/api/cndbpedia/ment2ent?q="

    val safeInterval = 1000L / 5

    @Volatile
    var lastRequestMs = nowMs()

    fun <ITEM> CNDBPediaRequest(url: String, keyword: String): List<ITEM> {
        synchronized(lastRequestMs) {
            if (nowMs() - lastRequestMs < safeInterval) {
                println("sleep for ${nowMs() - lastRequestMs} ms")
                Thread.sleep(nowMs() - lastRequestMs)
            }
            println("requesting $url$keyword")
            val result = (url + keyword)
                .httpGet().responseString().third.component1()!!
                .json2Map()
            if (!result.containsKey("ret")) {
                throw IllegalStateException("request for $url$keyword failed! response: $result")
            }
            return result["ret"] as List<ITEM>
        }
    }

    fun getEntitiesForMention(mention: String) = CNDBPediaRequest<String>(mentionUrl, mention).toSet()

    fun getTriplesForEntity(entity: String) = CNDBPediaRequest<List<String>>(tripleUrl, entity)

    init {
        client.executeSQL(
            "create table if not exists ment2ent(" +
                    "  \"q\" text NOT NULL,\n" +
                    "  \"response\" text NOT NULL,\n" +
                    "  \"create_time\" text NOT NULL,\n" +
                    "  PRIMARY KEY (\"q\"))"
        )
        client.executeSQL(
            "create table if not exists avpair(" +
                    "  \"q\" text NOT NULL,\n" +
                    "  \"response\" text NOT NULL,\n" +
                    "  \"create_time\" text NOT NULL,\n" +
                    "  PRIMARY KEY (\"q\"))"
        )
    }

    fun getFromDB(q: String, table: String): String? {
        val resultList = client.buildSelect("response")
            .from(table)
            .where("q", "=", q)
            .execute()
        return if (resultList.isEmpty()) {
            println("query $table:$q missed!")
            null
        } else {
            println("query $table:$q hit cache!")
            resultList.first()["response"] as String
        }
    }

    fun getMent2Ent(mention: String): Set<String> {
        val retFromCache = getFromDB(mention, "ment2ent")
        return if (retFromCache == null) {
            val entities = getEntitiesForMention(mention)
            saveMent2Ent(mention, ListHelper().toJson(entities.toList()))
            entities
        } else {
            ListHelper().fromJson(retFromCache).toSet() as Set<String>
        }
    }

    fun getAVPair(entity: String): List<List<String>> {
        val retFromCache = getFromDB(entity, "avpair")
        return if (retFromCache == null) {
            val triples = getTriplesForEntity(entity)
            saveAVPair(entity, ListHelper().toJson(triples))
            triples
        } else {
            ListHelper().fromJson(retFromCache) as List<List<String>>
        }
    }

    fun saveToDB(q: String, response: String, table: String) {
        client.insert(
            table,
            "q" to q,
            "response" to response,
            "create_time" to now()
        )
    }

    fun saveMent2Ent(q: String, response: String) = saveToDB(q, response, "ment2ent")

    fun saveAVPair(q: String, response: String) = saveToDB(q, response, "avpair")

}