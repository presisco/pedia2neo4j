package com.presisco.pedia2neo4j.nell995

import com.presisco.pedia2neo4j.Neo4jGraph
import com.presisco.pedia2neo4j.Neo4jIdCache
import org.slf4j.LoggerFactory
import java.io.File

object FromText : Neo4jGraph() {
    val logger = LoggerFactory.getLogger(FromText::class.java)

    val conceptRegex = "concept_(.+?)_(.+)".toRegex()
    val relationRegex = "concept:(.+)".toRegex()

    fun String.getLabelAndEntity(): Pair<String, String> {
        try {
            val values = conceptRegex.find(this)!!.groupValues
            return Pair(values[1], values[2])
        } catch (e: Exception) {
            return Pair(this, "gpslocation")
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        clearGraph()
        val triplesFile = if (args.isNotEmpty()) {
            args[0]
        } else {
            "C:\\projects\\DeepPath-master\\NELL-995\\kb_env_rl.txt"
        }
        val reader = File(triplesFile).bufferedReader()
        var line = reader.readLine()
        while (line != null) {
            val items = line.split("\t")
            val fromItem = items[0]
            val toItem = items[1]
            val relation = relationRegex.find(items[2])!!.groupValues[1]

            val (fromLabel, fromEntity) = fromItem.getLabelAndEntity()
            val fromId = mergeEntityWithCache(fromEntity, setOf(fromLabel))

            val (toLabel, toEntity) = toItem.getLabelAndEntity()
            val toId = mergeEntityWithCache(toEntity, setOf(toLabel))

            createRelationBetweenIds(fromId, toId, relation)

            line = reader.readLine()
        }
        logger.info("finished! wrote ${Neo4jIdCache.idList.size} nodes!")
        closeGraph()
    }

}