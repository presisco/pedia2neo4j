package com.presisco.pedia2neo4j.cndbpedia

import com.presisco.pedia2neo4j.Neo4jGraph
import com.presisco.pedia2neo4j.createRelationBetweenIds

abstract class AbstractSubjectGraph : Neo4jGraph() {
    companion object {
        const val entityLabelRelation = "CATEGORY_ZH"
    }

    val finishedSet = hashSetOf<String>()

    abstract fun targetLabelsForRelation(labels: Set<String>, relation: String): Set<String>

    abstract fun isRecursive(labels: Set<String>, relation: String): Boolean

    fun startWithEntity(entity: String) {
        val triples = APIRequestCache.getAVPair(entity)
        val labels = triples.filter { it[0] == entityLabelRelation }.map { it[1] }.toSet()
        println("labels for $entity: $labels")

        val fromId = mergeEntityWithCache(entity, labels)

        triples.forEach { triple ->
            val relation = triple[0]
            val toMention = triple[1]
            println("creating '$entity' --'$relation'--> '$toMention'")
            if (isRecursive(labels, relation)) {
                startWithMention(toMention)
            }
            val toLabels = targetLabelsForRelation(labels, relation)
            val toId = mergeEntityWithCache(toMention, toLabels)
            session.createRelationBetweenIds(fromId, toId, relation)
        }
    }

    fun startWithMention(mention: String) {
        val seedSet = APIRequestCache.getMent2Ent(mention)

        val queueSet = seedSet.minus(finishedSet)
        queueSet.forEach { seedWord ->
            finishedSet.add(seedWord)
            startWithEntity(seedWord)
        }

    }

}