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

    @Test
    fun extractTime() {
        expect("2019-07-01 14:43") {
            "<div class=\"WB_from S_txt2\"><a title=\"2019-07-01 14:43\">7月1日 14:43</a></div>"
                .extractValues(FromScrappyDump.timeFromXml)
                .first()
        }
        expect("08月21日 22:40") {
            ("<a href=\"//weibo.com/2803301701/I38U0qh0y?refer_flag=1001030103_\" " +
                    "target=\"_blank\" suda-data=\"key=tblog_search_weibo&amp;" +
                    "value=seqid:156916441389001236229|type:1|t:0|pos:1-0|" +
                    "q:%23%E8%BF%99%E6%89%8D%E6%98%AF%E9%A6%99%E6%B8%AF%E5%BA%94%E6%9C%89%E7%9A%84%E6%A0%B7%E5%AD%90%23|" +
                    "ext:cate:306,mpos:1,click:wb_time\">08月21日 22:40 转赞人数超过10万;</a>")
                .extractValues(FromScrappyDump.timeFromXmlText)
                .first()
                .substringBefore(" 转赞人数")
        }
    }

    @Test
    fun formatTime() {
        expect("2019-09-15 04:05") {
            FromScrappyDump.alignTime("2019-09-15 04:05:06", "刚刚")
        }
        expect("2019-09-15 04:05") {
            FromScrappyDump.alignTime("2019-09-15 04:05:06", "6秒前")
        }
        expect("2019-09-15 03:32") {
            FromScrappyDump.alignTime("2019-09-15 04:05:06", "33分钟前 转赞人数超过10")
        }
        expect("2019-09-15 02:05") {
            FromScrappyDump.alignTime("2019-09-15 04:05:06", "2小时前")
        }
        expect("2019-09-15 14:38") {
            FromScrappyDump.alignTime("2019-09-15 04:05:06", "今天 14:38")
        }
        expect("2019-09-15 14:38") {
            FromScrappyDump.alignTime("2019-09-15 04:05:06", "今天14:38 转赞人数超过10")
        }
        expect("2019-08-24 12:42") {
            FromScrappyDump.alignTime("2019-09-15 04:05:06", "08月01日 12:14")
        }
    }

    @Test
    fun extractName() {
        expect("加措上师-慈爱基金") {
            "<a href=\"//weibo.com/1342829361?refer_flag=1001030103_\" class=\"name\" target=\"_blank\" nick-name=\"加措上师-慈爱基金\" suda-data=\"key=tblog_search_weibo&amp;value=seqid:156897001871702075658|type:1|t:0|pos:1-0|q:%E8%97%8F%E4%BC%A0%E4%BD%9B%E6%95%99|ext:cate:26,mpos:1,click:user_name\">加措上师-慈爱基金</a>".extractValues(
                FromScrappyDump.nicknameRegex
            ).first()
        }
    }

}