package com.presisco.pedia2neo4j.weibo

import com.presisco.pedia2neo4j.extractValues
import com.presisco.pedia2neo4j.firstMatch
import org.junit.Test
import kotlin.test.expect

class ExtractTest {

    @Test
    fun extractNumbers() {
        expect(17376) { "转发 17376".firstMatch(FromScrappyDump.numberRegex)!!.toInt() }
        expect(17376) { "点赞 17376".firstMatch(FromScrappyDump.numberRegex)!!.toInt() }
        expect(17376) { "评论 17376".firstMatch(FromScrappyDump.numberRegex)!!.toInt() }
    }

    @Test
    fun extractTags() {
        expect(listOf("@abc", "@def")) {
            "@abc 一些内容 @def//@ghi: 另一些内容//@jkl: 还有一些内容"
                .substringBefore("//@")
                .extractValues(FromScrappyDump.quoteUserRegex)
        }
        expect(listOf("#this#", "#that#")) {
            "#this#and#that#//@someone: #another# thing"
                .substringBefore("//@")
                .extractValues(FromScrappyDump.topicRegex)
        }
    }

}