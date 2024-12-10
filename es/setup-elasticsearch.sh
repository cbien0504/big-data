#!/bin/bash

curl -X PUT "elasticsearch:9200/tweets" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "author": {
        "type": "keyword"
      },
      "authorName": {
        "type": "keyword"
      },
      "created": {
        "type": "date",
        "format": "yyyy-MM-dd HH:mm:ss.SSSZ"
      },
      "hashTags": {
        "type": "keyword"
      },
      "likes": {
        "type": "long"
      },
      "replyCounts": {
        "type": "long"
      },
      "retweetCounts": {
        "type": "long"
      },
      "retweetedTweet": {
        "type": "nested",
        "properties": {
          "_id": {
            "type": "keyword"
          },
          "author": {
            "type": "keyword"
          },
          "authorName": {
            "type": "keyword"
          },
          "created": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss.SSSZ"
          },
          "hashTags": {
            "type": "keyword"
          },
          "likes": {
            "type": "long"
          },
          "replyCounts": {
            "type": "long"
          },
          "retweetCounts": {
            "type": "long"
          },
          "retweetedTweet": {
            "type": "keyword"
          },
          "text": {
            "type": "text"
          },
          "timestamp": {
            "type": "long"
          },
          "url": {
            "type": "keyword"
          },
          "userMentions": {
            "type": "nested"
          },
          "views": {
            "type": "long"
          }
        }
      },
      "text": {
        "type": "text"
      },
      "timestamp": {
        "type": "long"
      },
      "url": {
        "type": "keyword"
      },
      "userMentions": {
        "type": "nested"
      },
      "views": {
        "type": "long"
      }
    }
  }
}
'
